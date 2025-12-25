import json
import os
from collections import defaultdict

HAR_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "har_reference")
OUTPUT_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), "docs", "har_contract.md")


def iter_har_files():
    for name in os.listdir(HAR_DIR):
        if not name.endswith(".txt"):
            continue
        yield os.path.join(HAR_DIR, name)


def load_entries():
    entries = []
    for path in iter_har_files():
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        entries.extend(data.get("log", {}).get("entries", []))
    return entries


def build_contract(entries):
    endpoints = defaultdict(set)
    headers = defaultdict(set)
    for entry in entries:
        request = entry.get("request", {})
        method = request.get("method")
        url = request.get("url", "")
        if "/AllocationChomage/api/" not in url:
            continue
        path = url.split("/AllocationChomage/api/")[-1].split("?")[0]
        endpoints[path].add(method)
        for header in request.get("headers", []):
            headers[path].add(header.get("name", ""))

    return endpoints, headers


def write_markdown(endpoints, headers):
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as handle:
        handle.write("# API Contract from HAR\n\n")
        handle.write("## Endpoints\n")
        for path in sorted(endpoints.keys()):
            methods = ", ".join(sorted(endpoints[path]))
            handle.write(f"- `{path}` ({methods})\n")
        handle.write("\n## Required Headers (per endpoint)\n")
        for path in sorted(headers.keys()):
            header_list = ", ".join(sorted(h for h in headers[path] if h))
            handle.write(f"- `{path}`: {header_list}\n")


def main():
    entries = load_entries()
    endpoints, headers = build_contract(entries)
    write_markdown(endpoints, headers)


if __name__ == "__main__":
    main()
