from typing import List

import random

from redis_bandit.base import Arm, Bandit


class EpsilonArm(Arm):
    count: int = 0
    estimated_reward: float = 0

    def update(self, reward: float) -> None:
        with self.lock():
            n = self.incr("count")
            # weighted average of the previously estimated value and the reward we just received
            self.estimated_reward = ((n - 1) / float(n)) * self.estimated_reward + (
                1 / float(n)
            ) * reward


class EpsilonGreedyBandit(Bandit[EpsilonArm]):
    def __init__(self, epsilon: float, redis_url: str, prefix: str) -> None:
        super().__init__(redis_url, prefix, EpsilonArm)
        self._epsilon = epsilon

    def select_arm(self, arm_ids: List[str]) -> str:
        if random.random() > self._epsilon:
            estimated_rewards = self.get_field_from_arms(arm_ids, "estimated_reward")
            i = max(range(len(arm_ids)), key=lambda i: estimated_rewards[i])  # type: ignore[no-any-return]
        else:
            i = random.randrange(len(arm_ids))
        return arm_ids[i]

    def rank_arms(self, arm_ids: List[str], k: int) -> List[str]:
        estimated_rewards = self.get_field_from_arms(arm_ids, "estimated_reward")
        sorted_arm_ids = [
            arm_id
            for _, arm_id in sorted(
                zip(estimated_rewards, arm_ids), key=lambda pair: pair[0], reverse=True  # type: ignore[no-any-return]
            )
        ]

        ranked_arms = []
        for _ in range(min(k, len(arm_ids))):
            if random.random() > self._epsilon:
                i = 0
            else:
                i = random.randrange(len(sorted_arm_ids))
            ranked_arms.append(sorted_arm_ids.pop(i))
        return ranked_arms
