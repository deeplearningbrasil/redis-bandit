import os
import pickle
import random
import tempfile

from redislite.patch import patch_redis

patch_redis()

from walrus import Database

from redis_bandit.base import Arm, Bandit


class CustomArm(Arm):
    count: int = 0


class CustomBandit(Bandit[CustomArm]):
    def __init__(self, redis_url: str, prefix: str) -> None:
        super().__init__(redis_url, prefix, CustomArm)

    @property
    def db(self) -> Database:
        if not hasattr(self, "_lazy_db"):
            # self._lazy_db = Database(
            #     connection_pool=ConnectionPool.from_url(self._redis_url, decode_responses=True)
            # )
            self._lazy_db = Database(decode_responses=True)
        return self._lazy_db


def test_new_arm_with_default_fields():
    db = Database(decode_responses=True)

    arm = CustomArm(db, "arms:1")
    assert arm.id == "1"
    assert arm.count == 0


def test_new_arm_with_custom_fields():
    db = Database(decode_responses=True)

    arm = CustomArm(db, "arms:1", count=10)
    assert arm.id == "1"
    assert arm.count == 10


def test_change_arm():
    db = Database(decode_responses=True)

    arm = CustomArm(db, "arms:1")
    same_arm1 = CustomArm(db, "arms:1")

    arm.count = 1
    assert arm.count == 1
    arm.count += 1
    assert arm.count == 2
    assert arm.incr("count", 1) == 3
    assert arm.count == 3

    same_arm2 = CustomArm(db, "arms:1")
    assert arm.count == same_arm1.count
    assert arm.count == same_arm2.count


def test_convert_arm_to_dict():
    db = Database(decode_responses=True)

    arm = CustomArm(db, "arms:1")

    assert arm.dict() == {"id": "1", "count": 0}

    arm.count = 10

    assert arm.dict() == {"id": "1", "count": 10}


def test_bandit_arm_ops():
    bandit = CustomBandit("redis://localhost:6374", "bandit")

    assert len(bandit.arm_ids) == 0
    assert len(bandit.arms) == 0

    arm1 = bandit.add_arm("1")
    assert arm1.id == "1"

    assert set(bandit.arm_ids) == {"1"}
    assert len(bandit.arms) == 1
    assert len(bandit) == 1
    assert list(bandit.arms)[0].id == arm1.id
    assert bandit["1"].id == arm1.id
    assert list(bandit.arms)[0].count == arm1.count
    assert bandit["1"].count == arm1.count
    arm1.count += 1
    assert list(bandit.arms)[0].count == arm1.count
    assert bandit["1"].count == arm1.count

    bandit.add_arm("2")
    bandit.add_arm("3")

    assert len(bandit.arms) == 3
    assert len(bandit) == 3
    assert set(bandit.arm_ids) == {"1", "2", "3"}
    assert {arm.id for arm in bandit.arms} == {"1", "2", "3"}

    bandit.delete_arm("2")

    assert len(bandit.arms) == 2
    assert len(bandit) == 2
    assert set(bandit.arm_ids) == {"1", "3"}
    assert {arm.id for arm in bandit.arms} == {"1", "3"}


def test_pickle_bandit():
    bandit = CustomBandit("redis://localhost:6374", "bandit")
    arm1 = bandit.add_arm("1")
    arm1.count += 1

    with tempfile.TemporaryDirectory() as dir_path:
        with open(os.path.join(dir_path, "bandit.pkl"), "wb") as f:
            pickle.dump(bandit, f)
        with open(os.path.join(dir_path, "bandit.pkl"), "rb") as f:
            restored_bandit = pickle.load(f)
        restored_bandit._lazy_db = (
            bandit.db
        )  # Necessary for redislite to connect to the same Redis

        assert list(bandit.arms)[0].id == arm1.id
        assert list(bandit.arms)[0].count == arm1.count

        assert list(restored_bandit.arms)[0].id == arm1.id
        assert list(restored_bandit.arms)[0].count == arm1.count


def test_get_field_from_arms():
    bandit = CustomBandit("redis://localhost:6374", "bandit")
    arm_ids = [str(i) for i in range(100)]
    count_values = [random.randint(0, 100) for _ in arm_ids]

    for arm_id, count_value in zip(arm_ids, count_values):
        arm = bandit.add_arm(arm_id)
        arm.count = count_value

    assert bandit.get_field_from_arms(arm_ids, "count") == count_values
    assert bandit.get_field_from_arms(arm_ids[:10], "count") == count_values[:10]
    assert bandit.get_field_from_arms(arm_ids[0::2], "count") == count_values[0::2]


# TODO: Add tests for Arm deletion
