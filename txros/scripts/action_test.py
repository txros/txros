#!/usr/bin/python3

import asyncio
import uvloop
import random

import txros
from txros import action

from actionlib.msg import TestAction, TestGoal


async def main():
    nh = await txros.NodeHandle.from_argv("action_test_node", anonymous=True)

    ac = action.ActionClient(nh, "test_action", TestAction)

    x = random.randrange(1000)
    goal_manager = ac.send_goal(
        TestGoal(
            goal=x,
        )
    )
    print("sent goal")

    while True:
        result, index = await asyncio.gather(
            goal_manager.get_feedback(), goal_manager.get_result()
        )

        if index == 0:  # feedback
            print("got feedback", result.feedback)
        else:
            assert index == 1  # result

            assert result.result == x + 1000, result.result
            print("success")

            break

if __name__ == "__main__":
    uvloop.install()
    asyncio.run(main())
