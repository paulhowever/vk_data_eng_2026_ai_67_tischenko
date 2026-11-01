from collections import Counter
from types import SimpleNamespace

from kazoo.exceptions import BadVersionError, ConnectionLoss, NodeExistsError, NoNodeError, RolledBackError
from kazoo.protocol.states import EventType, KeeperState, WatchedEvent

from hhlake.mr import engine, jobs
from hhlake.zk import common, coord

PATHS = common.Paths("test")


class FakeTx:
    def __init__(self, zk):
        self.zk = zk
        self.ops = []

    def check(self, path, version):
        self.ops.append(("check", path, version))

    def set_data(self, path, value):
        self.ops.append(("set", path, value))

    def failure(self, path, version):
        if path not in self.zk.nodes:
            return NoNodeError()
        if self.zk.nodes[path][1] != version:
            return BadVersionError()
        return None

    def commit(self):
        for op, path, arg in self.ops:
            error = op == "check" and self.failure(path, arg)
            if error:
                return [error if o[1] == path else RolledBackError() for o in self.ops]
        return [True if op == "check" else self.zk.set(path, arg) for op, path, arg in self.ops]


class FakeZK:
    def __init__(self):
        self.nodes = {PATHS.epoch: (b"", 0), PATHS.result: (b"", 0)}
        self.seq = 0
        self.lose_reply = False
        self.script = []

    def retry(self, fn, *args, **kwargs):
        return fn(*args, **kwargs)

    def set(self, path, value):
        version = self.nodes[path][1] + 1
        self.nodes[path] = (value, version)
        return SimpleNamespace(version=version)

    def create(self, path, value, ephemeral=False, sequence=False):
        if sequence:
            path = f"{path}{self.seq:010d}"
            self.seq += 1
        if path in self.nodes:
            raise NodeExistsError()
        self.nodes[path] = (value, 0)
        if self.lose_reply:
            self.lose_reply = False
            raise ConnectionLoss()
        return path

    def get(self, path, watch=None):
        if path not in self.nodes:
            raise NoNodeError()
        value, version = self.nodes[path]
        if watch:
            self.script.pop(0)(watch)
        return value, SimpleNamespace(version=version)

    def drop(self, path):
        del self.nodes[path]
        return WatchedEvent(EventType.DELETED, KeeperState.CONNECTED, path)

    def get_children(self, path):
        return [node.rsplit("/", 1)[1] for node in self.nodes if node.rsplit("/", 1)[0] == path]

    def transaction(self):
        return FakeTx(self)


LOG = SimpleNamespace(info=lambda message: None)


def test_epoch_is_taken_only_while_own_election_node_exists():
    zk = FakeZK()
    a = coord.join(zk, PATHS, "A", LOG)
    b = coord.join(zk, PATHS, "B", LOG)
    assert coord.take_epoch(zk, PATHS, a, "A") == 1
    del zk.nodes[f"{PATHS.election}/{a}"]
    assert coord.take_epoch(zk, PATHS, b, "B") == 2
    assert coord.take_epoch(zk, PATHS, a, "A") is None
    assert zk.nodes[PATHS.epoch] == (b"B", 2)


def test_lost_reply_does_not_leave_second_election_node():
    zk = FakeZK()
    zk.lose_reply = True
    mine = coord.join(zk, PATHS, "A", LOG)
    assert zk.get_children(PATHS.election) == [mine]
    assert mine.endswith("-n_0000000000")


def test_election_order_uses_sequence_suffix_not_prefix():
    nodes = ["ffff-n_0000000003", "0000-n_0000000007", "aaaa-n_0000000001"]
    assert coord.order(nodes) == ["aaaa-n_0000000001", "ffff-n_0000000003", "0000-n_0000000007"]


def test_backup_rechecks_on_session_events_and_waits_for_real_delete():
    zk = FakeZK()
    ahead = f"{PATHS.election}/{coord.join(zk, PATHS, 'A', LOG)}"
    lines = []
    zk.script = [lambda watch: watch(WatchedEvent(EventType.NONE, KeeperState.CONNECTING, None)),
                 lambda watch: watch(zk.drop(ahead))]
    _, epoch = coord.elect(zk, PATHS, "B", SimpleNamespace(info=lines.append))
    assert epoch == 1
    assert sum("удалён" in line for line in lines) == 1
    assert sum("запасной" in line for line in lines) == 1


def test_backup_rejoins_when_its_session_expired():
    zk = FakeZK()
    coord.join(zk, PATHS, "A", LOG)
    lines = []

    def expire(watch):
        for node in zk.get_children(PATHS.election):
            zk.drop(f"{PATHS.election}/{node}")
        watch(WatchedEvent(EventType.NONE, KeeperState.EXPIRED_SESSION, None))

    zk.script = [expire]
    mine, epoch = coord.elect(zk, PATHS, "B", SimpleNamespace(info=lines.append))
    assert epoch == 1
    assert zk.get_children(PATHS.election) == [mine]
    assert any("заново" in line for line in lines)
    assert not any("удалён" in line for line in lines)


def test_new_leader_skips_checkpointed_tasks():
    tasks = [engine.MapTask(i, 0, 0) for i in range(5)]
    assert [t.index for t in coord.pending(tasks, {0: {}, 1: {}, 3: {}})] == [2, 4]


def test_checkpoint_entry_is_written_once():
    zk = FakeZK()
    assert coord.save(zk, PATHS, {"task": 7, "by": "B"})
    assert not coord.save(zk, PATHS, {"task": 7, "by": "A"})
    assert b'"B"' in zk.nodes[PATHS.task(7)][0]


def test_reduce_reads_parts_written_by_both_leaders():
    store = {}
    entries = []
    batches = [Counter({("mysql", "php"): 2, ("git", "php"): 1}), Counter({("mysql", "php"): 3})]
    for task, (epoch, counts) in enumerate(zip((1, 2), batches)):
        blobs, _ = jobs.shuffle_blobs({common.K: counts}, common.REDUCERS)
        keys = []
        for r, blob in blobs.items():
            key = f"tmp/e{epoch}/map-{task}/part-{r}"
            store[key] = blob
            keys.append(key)
        entries.append({"task": task, "keys": keys})
    assert coord.combine(entries, store.__getitem__) == Counter({("mysql", "php"): 5, ("git", "php"): 1})


def test_without_fencing_stale_leader_overwrites_pointer():
    zk = FakeZK()
    zk.set(PATHS.epoch, b"A")
    zk.set(PATHS.epoch, b"B")
    assert coord.publish(zk, PATHS, b"B", 2, False) == (True, 1)
    assert coord.publish(zk, PATHS, b"A", 1, False) == (True, 2)
    assert zk.nodes[PATHS.result] == (b"A", 2)


def test_with_fencing_stale_leader_is_rejected():
    zk = FakeZK()
    zk.set(PATHS.epoch, b"A")
    zk.set(PATHS.epoch, b"B")
    assert coord.publish(zk, PATHS, b"B", 2, True) == (True, 1)
    assert coord.publish(zk, PATHS, b"A", 1, True) == (False, "BadVersionError")
    assert zk.nodes[PATHS.result] == (b"B", 1)
