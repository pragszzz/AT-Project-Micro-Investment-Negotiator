#!/usr/bin/env python3
"""
start_all.py - one command to run the whole system.

    pip install -r requirements.txt
    python start_all.py

Starts the Broker agent (port 8001) and the Buyer agent (port 8000), waits until
the Buyer answers, then opens the live dashboard in your browser.
Press Ctrl+C to stop both.
"""

import os
import subprocess
import sys
import time
import webbrowser

import requests

HERE = os.path.dirname(os.path.abspath(__file__))
DASHBOARD_URL = "http://localhost:8000"


def wait_until_up(url, timeout=25):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if requests.get(url, timeout=1).status_code == 200:
                return True
        except requests.exceptions.RequestException:
            time.sleep(0.4)
    return False


def main():
    print("=== Micro-Investment Negotiator Agent ===\n")
    procs = []
    for name, script in (("Broker agent (port 8001)", "broker_agent.py"), ("Buyer agent  (port 8000)", "agent_server.py")):
        print(f"starting {name} ...")
        procs.append((name, subprocess.Popen([sys.executable, script], cwd=HERE)))
        time.sleep(1.2)

    if not wait_until_up(DASHBOARD_URL + "/state"):
        print("\nThe Buyer agent did not come up. Check the error above (is another program using port 8000/8001?).")
        for _, p in procs:
            p.terminate()
        sys.exit(1)

    print(f"\nSystem ready. Dashboard: {DASHBOARD_URL}")
    print("Prices show LIVE when the market APIs are reachable, otherwise SIMULATED (clearly labelled).")
    print("Press Ctrl+C to stop.\n")
    if not os.getenv("NO_BROWSER"):
        webbrowser.open(DASHBOARD_URL)

    try:
        while True:
            for name, p in procs:
                if p.poll() is not None:
                    print(f"[warning] {name} stopped (exit code {p.returncode})")
                    raise KeyboardInterrupt
            time.sleep(2)
    except KeyboardInterrupt:
        print("\nstopping...")
        for _, p in procs:
            p.terminate()
        for _, p in procs:
            p.wait()
        print("stopped.")


if __name__ == "__main__":
    main()
