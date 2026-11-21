#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cross-datacenter syncrepl partition tests.

The tests run a two-node multiprovider and cover the three failure modes that
matter for active/active directory deployments:

* concurrent writes to the same attribute,
* a delete racing a modify,
* a node restarting after a network partition and resuming from its cookie.
"""

import ldap
import os
import signal
import time

import pytest

from .syncrepl import mmr, search_one, wait_for_resync, wait_until


SUFFIX = "dc=example,dc=com"
ENTRY_DN = f"cn=partition,{SUFFIX}"
ADD_DN = f"cn=add-conflict,{SUFFIX}"


def _root_conn(server):
    db = server.suffixes[SUFFIX]
    conn = server.connect()
    conn.simple_bind_s(db.rootdn, db.secret)
    return conn


def _config_conn(server):
    conn = server.connect()
    conn.simple_bind_s("cn=config", server.secret)
    return conn


def _ensure_base(servers):
    conn = _root_conn(next(iter(servers.values())))
    try:
        conn.add_s(SUFFIX, [
            ("objectClass", [b"organization", b"domainRelatedObject",
                             b"dcObject"]),
            ("o", [b"Example, Inc."]),
            ("dc", [b"example"]),
            ("associatedDomain", [b"example.com"]),
        ])
    except ldap.ALREADY_EXISTS:
        pass
    finally:
        conn.unbind_s()
    wait_for_resync(SUFFIX, servers.values())


def _add_employee_entry(server, dn, employee_number):
    conn = _root_conn(server)
    try:
        conn.add_s(dn, [
            ("objectClass", [b"person", b"organizationalPerson",
                             b"inetOrgPerson"]),
            ("cn", [dn.split(",", 1)[0].split("=", 1)[1].encode()]),
            ("sn", [b"Partition"]),
            ("employeeNumber", [employee_number]),
        ])
    except ldap.ALREADY_EXISTS:
        pass
    finally:
        conn.unbind_s()


def _add_entry(servers):
    _add_employee_entry(next(iter(servers.values())), ENTRY_DN, b"1000")
    wait_for_resync(SUFFIX, servers.values())


def _pause_replication(server):
    conn = _config_conn(server)
    db = server.suffixes[SUFFIX]
    try:
        _, attrs = conn.search_s(db.dn, ldap.SCOPE_BASE, "(objectClass=*)",
                                 ["olcSyncrepl"])[0]
        values = attrs.get("olcSyncrepl", [])
        if values:
            conn.modify_s(db.dn,
                          [(ldap.MOD_DELETE, "olcSyncrepl", None)])
    finally:
        conn.unbind_s()
    return db.dn, values


def _restore_replication(state):
    dn, values, server = state
    if not values:
        return
    conn = _config_conn(server)
    try:
        conn.modify_s(dn, [(ldap.MOD_REPLACE, "olcSyncrepl", values)])
    finally:
        conn.unbind_s()


def _partition(servers):
    states = {}
    for serverid, server in servers.items():
        states[serverid] = _pause_replication(server)
        states[serverid] += (server,)

    for server in servers.values():
        server.stop()
    for server in servers.values():
        server.start()

    # The restart must not silently reload the old replication config.
    for server in servers.values():
        conn = _config_conn(server)
        try:
            _, attrs = conn.search_s(server.suffixes[SUFFIX].dn,
                                     ldap.SCOPE_BASE, "(objectClass=*)",
                                     ["olcSyncrepl"])[0]
            assert not attrs.get("olcSyncrepl")
        finally:
            conn.unbind_s()

    return states


def _restore(servers, states):
    for serverid, state in states.items():
        _restore_replication(state)
    wait_for_resync(SUFFIX, servers.values())


def _modify_employee_number(server, value):
    conn = _root_conn(server)
    try:
        conn.modify_s(ENTRY_DN,
                      [(ldap.MOD_REPLACE, "employeeNumber",
                        [value.encode()])])
    finally:
        conn.unbind_s()


def _delete_entry(server):
    conn = _root_conn(server)
    try:
        conn.delete_s(ENTRY_DN)
    finally:
        conn.unbind_s()


def _entry_csn(server, dn=ENTRY_DN):
    db = server.suffixes[SUFFIX]
    _, attrs = search_one(server, dn, ["entryCSN"],
                          rootdn=db.rootdn, secret=db.secret)
    return attrs["entryCSN"][0].decode()


def _context_csn_for_sid(server, serverid):
    db = server.suffixes[SUFFIX]
    _, attrs = search_one(server, SUFFIX, ["contextCSN"],
                          rootdn=db.rootdn, secret=db.secret)
    sid = f"{serverid:03d}"
    values = [value.decode() for value in attrs["contextCSN"]]
    matching = [value for value in values if value.split("#")[2] == sid]
    assert matching, f"no contextCSN for sid {sid}: {values!r}"
    return max(matching)


def _employee_number(server, dn=ENTRY_DN):
    conn = _root_conn(server)
    try:
        result = conn.search_s(dn, ldap.SCOPE_BASE, "(objectClass=*)",
                               ["employeeNumber"])
    except ldap.NO_SUCH_OBJECT:
        return None
    finally:
        conn.unbind_s()
    return result[0][1]["employeeNumber"][0] if result else None


def _wait_employee_number(servers, expected, dn=ENTRY_DN):
    wait_until(lambda: all(_employee_number(server, dn) == expected
                           for server in servers.values()))


def _log_text(server):
    try:
        return (server.home / "slapd.log").read_text(errors="replace")
    except FileNotFoundError:
        return ""


def _wait_for_audit(servers, *needles):
    def found():
        logs = "\n".join(_log_text(server) for server in servers.values())
        return all(needle in logs for needle in needles)

    wait_until(found, timeout=10)


def _enable_checkpoint(server):
    conn = _config_conn(server)
    try:
        conn.modify_s(server.syncprov.dn,
                      [(ldap.MOD_ADD, "olcSpCheckpoint", [b"1 1"])])
    finally:
        conn.unbind_s()


def _crash(server):
    os.kill(server.pid, signal.SIGKILL)
    server.process.wait()
    server.process = None


@pytest.mark.mmr(mmr=2)
class TestSameAttributePartition:
    def test_predictable_last_writer_wins(self, mmr):
        _ensure_base(mmr)
        _add_entry(mmr)

        states = _partition(mmr)
        server_a = mmr[1]
        server_b = mmr[2]

        _modify_employee_number(server_a, "1001")
        csn_a = _entry_csn(server_a)
        _modify_employee_number(server_b, "2002")
        csn_b = _entry_csn(server_b)

        _add_employee_entry(server_a, ADD_DN, b"3003")
        add_csn_a = _entry_csn(server_a, ADD_DN)
        _add_employee_entry(server_b, ADD_DN, b"4004")
        add_csn_b = _entry_csn(server_b, ADD_DN)

        expected = b"1001" if csn_a > csn_b else b"2002"
        add_expected = b"3003" if add_csn_a > add_csn_b else b"4004"
        _restore(mmr, states)
        _wait_employee_number(mmr, expected)
        _wait_employee_number(mmr, add_expected, ADD_DN)

        _wait_for_audit(mmr, "syncrepl audit:", "conflict-")


@pytest.mark.mmr(mmr=2)
class TestDeleteModifyPartition:
    def test_newer_modify_survives_older_delete(self, mmr):
        _ensure_base(mmr)
        _add_entry(mmr)

        states = _partition(mmr)
        server_a = mmr[1]
        server_b = mmr[2]

        _delete_entry(server_a)
        delete_csn = _context_csn_for_sid(server_a, 1)
        _modify_employee_number(server_b, "2002")
        modify_csn = _entry_csn(server_b)
        assert modify_csn > delete_csn

        _restore(mmr, states)
        _wait_employee_number(mmr, b"2002")

        _wait_for_audit({2: server_b}, "syncrepl audit:")


@pytest.mark.mmr(mmr=2)
class TestRestartResumePartition:
    def test_consumer_resumes_after_crash(self, mmr):
        _ensure_base(mmr)
        _add_entry(mmr)
        server_a = mmr[1]
        server_b = mmr[2]

        _enable_checkpoint(server_a)
        _modify_employee_number(server_a, "1001")
        wait_until(lambda: _employee_number(server_b) == b"1001")
        time.sleep(0.5)
        _crash(server_a)

        added = []
        conn = _root_conn(server_b)
        try:
            for index in range(5):
                dn = f"cn=resume{index},{SUFFIX}"
                conn.add_s(dn, [
                    ("objectClass", [b"device"]),
                    ("description", [f"written while A was down "
                                     f"({index})".encode()]),
                ])
                added.append(dn)
        finally:
            conn.unbind_s()

        server_a.start()
        wait_until(lambda: all(
            search_one(server_a, dn, ["1.1"])[0] == dn for dn in added))
        wait_for_resync(SUFFIX, mmr.values())
