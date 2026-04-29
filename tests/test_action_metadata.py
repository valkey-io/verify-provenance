#!/usr/bin/env python3
"""
Tests for the GitHub Action metadata contract documented in README.md.
"""

import os
import unittest


ACTION_YML = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "action.yml")


def read_action_inputs():
    inputs = {}
    current = None
    in_inputs = False

    with open(ACTION_YML, encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.rstrip("\n")
            if line == "inputs:":
                in_inputs = True
                continue
            if in_inputs and line and not line.startswith(" "):
                break
            if not in_inputs:
                continue

            if line.startswith("  ") and not line.startswith("    ") and line.strip().endswith(":"):
                current = line.strip()[:-1]
                inputs[current] = {}
                continue

            if current and line.startswith("    ") and ":" in line:
                key, value = line.strip().split(":", 1)
                inputs[current][key] = value.strip().strip('"').strip("'")

    return inputs


class TestActionMetadata(unittest.TestCase):
    def test_action_exposes_matching_configuration(self):
        inputs = read_action_inputs()

        self.assertIn("branding_pairs", inputs)
        self.assertIn("prefix_pairs", inputs)
        self.assertIn("exclude_dirs", inputs)

    def test_check_step_passes_excluded_directories(self):
        with open(ACTION_YML, encoding="utf-8") as f:
            action = f.read()

        self.assertIn('--exclude-dirs "${{ inputs.exclude_dirs }}"', action)

    def test_check_step_captures_exit_code_before_failing(self):
        with open(ACTION_YML, encoding="utf-8") as f:
            action = f.read()

        self.assertIn("set +e\n          python3 ${{ github.action_path }}/src/check.py", action)
        self.assertIn("EXIT_CODE=$?\n          set -e", action)


if __name__ == "__main__":
    unittest.main()
