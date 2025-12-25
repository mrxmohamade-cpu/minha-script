import unittest
import importlib.util
import os


def load_classifier_module():
    module_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "robot", "robot_classifier.py")
    spec = importlib.util.spec_from_file_location("robot_classifier", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class RobotClassifierTests(unittest.TestCase):
    def setUp(self):
        module = load_classifier_module()
        self.ResultType = module.ResultType
        self.classifier = module.ResultClassifier()

    def test_rate_limit(self):
        result = self.classifier.classify("فشل جلب التواريخ", "RATE_LIMIT_429", http_status=429)
        self.assertEqual(result, self.ResultType.RATE_LIMIT)

    def test_has_rdv(self):
        result = self.classifier.classify("لديه موعد مسبق", "")
        self.assertEqual(result, self.ResultType.HAS_RDV)

    def test_needs_preinscription(self):
        result = self.classifier.classify("يتطلب تسجيل مسبق", "")
        self.assertEqual(result, self.ResultType.NEEDS_PREINSCRIPTION)

    def test_no_dates(self):
        result = self.classifier.classify("لا توجد مواعيد", "")
        self.assertEqual(result, self.ResultType.NO_DATES)
