import logging
import os
from dataclasses import dataclass

from kazoo.client import KazooClient

from .. import config

HOSTS = os.environ.get("ZK_HOSTS", "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183")
TIMEOUT = 10.0
K = 2
REDUCERS = 4
BASELINE = f"{config.RESULTS_MR}/k{K}.csv"
LOGS = config.ROOT / "logs"
STAMP = "%Y-%m-%d %H:%M:%S"


@dataclass(frozen=True)
class Paths:
    run: str

    @property
    def root(self):
        return f"/hhlake/{self.run}"

    @property
    def election(self):
        return f"{self.root}/election"

    @property
    def epoch(self):
        return f"{self.root}/epoch"

    @property
    def done(self):
        return f"{self.root}/done"

    @property
    def result(self):
        return f"{self.root}/result"

    def task(self, index):
        return f"{self.done}/map-{index:03d}"


def num(value):
    return f"{value:,}".replace(",", " ")


def job_name(run, epoch):
    return f"zk-{run}-e{epoch}"


def tmp_prefix(run):
    return f"{config.TMP_MR}/zk-{run}-"


def result_prefix(run):
    return f"results/zk/{run}/"


def result_key(run, epoch):
    return f"{result_prefix(run)}epoch-{epoch}/k{K}.csv"


def client(timeout=TIMEOUT):
    retry = {"max_tries": -1, "delay": 0.2, "max_delay": 1.0}
    return KazooClient(hosts=HOSTS, timeout=timeout, connection_retry=retry, command_retry=retry)


def open_log(run, name):
    path = LOGS / run / f"{name}.log"
    path.parent.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(path, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s.%(msecs)03d %(name)-8s %(message)s", STAMP))
    root = logging.getLogger()
    root.addHandler(handler)
    root.setLevel(logging.WARNING)
    log = logging.getLogger(name)
    log.setLevel(logging.INFO)
    return log
