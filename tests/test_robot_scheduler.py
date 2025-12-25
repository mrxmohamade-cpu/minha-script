import unittest
import importlib.util
import sys
import os


def load_module(module_name, module_alias):
    module_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "robot", module_name)
    spec = importlib.util.spec_from_file_location(module_alias, module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    sys.modules[module_alias] = module
    return module


class RobotSchedulerTests(unittest.TestCase):
    def setUp(self):
        classifier_module = load_module("robot_classifier.py", "robot.robot_classifier")
        scheduler_module = load_module("robot_scheduler.py", "robot.robot_scheduler")
        self.ResultType = classifier_module.ResultType
        self.scheduler = scheduler_module.SmartScheduler(
            {
                "base_global_interval_sec": 8,
                "burst_duration_min": 25,
                "freeze_has_rdv_days": 7,
                "rate_limit_pause_min_sec": 2700,
                "rate_limit_pause_max_sec": 7200,
                "rate_limit_window_sec": 900,
                "rate_limit_threshold": 2,
            }
        )

    def test_round_limit(self):
        self.scheduler.start_round()
        member_id = "m1"
        self.assertTrue(self.scheduler.can_process(member_id, 0))
        self.scheduler.record_result(member_id, self.ResultType.NO_DATES)
        self.assertFalse(self.scheduler.can_process(member_id, 0))

    def test_rate_limit_pause(self):
        member_id = "m2"
        self.scheduler.record_result(member_id, self.ResultType.RATE_LIMIT)
        self.assertTrue(self.scheduler.is_paused())
