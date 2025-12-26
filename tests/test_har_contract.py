import json
import os
import unittest
import importlib.util


HAR_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "har_reference")


def load_entries():
    entries = []
    for name in os.listdir(HAR_DIR):
        if not name.endswith(".txt"):
            continue
        with open(os.path.join(HAR_DIR, name), "r", encoding="utf-8") as handle:
            data = json.load(handle)
        entries.extend(data.get("log", {}).get("entries", []))
    return entries


class HarContractTests(unittest.TestCase):
    def test_required_endpoints_present(self):
        entries = load_entries()
        urls = [entry.get("request", {}).get("url", "") for entry in entries]
        self.assertTrue(any("validateCandidate" in url for url in urls))
        self.assertTrue(any("PreInscription/GetPreInscription" in url for url in urls))
        self.assertTrue(any("RendezVous/GetAvailableDates" in url for url in urls))

    def test_core_headers_present(self):
        if importlib.util.find_spec("requests") is None:
            self.skipTest("requests not available")
        from config import SESSION
        expected_headers = {"User-Agent", "Accept", "Origin", "Referer"}
        session_headers = {key for key in SESSION.headers.keys()}
        missing = expected_headers - session_headers
        self.assertFalse(missing, f"Missing headers: {missing}")
