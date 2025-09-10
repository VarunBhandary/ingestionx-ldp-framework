#!/usr/bin/env python3
"""
Test runner for the autoloader-framework-pydab project

This script runs the expression parser tests to ensure the parsing logic
works correctly with various Spark SQL expression patterns.

Usage:
    python run_tests.py                    # Run all tests
    python run_tests.py --verbose          # Run with verbose output
    python run_tests.py --coverage         # Run with coverage report
"""

import sys
import os
import subprocess
import argparse

def run_tests(verbose=False, coverage=False, specific_test=None):
    """Run the test suite"""
    
    # Add the current directory to Python path
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    
    # Build pytest command
    cmd = ["python", "-m", "pytest"]
    
    if verbose:
        cmd.append("-v")
    
    if coverage:
        cmd.extend(["--cov=resources", "--cov=src", "--cov-report=html", "--cov-report=term"])
    
    # Add specific test if provided, otherwise run all tests
    if specific_test:
        cmd.append(f"tests/{specific_test}")
        print(f"Running specific test: {specific_test}")
    else:
        cmd.append("tests/")
        print("Running all test suites...")
    
    print(f"Command: {' '.join(cmd)}")
    print("=" * 60)
    
    try:
        result = subprocess.run(cmd, check=True)
        print("\n" + "=" * 60)
        print("✅ All tests passed!")
        return True
    except subprocess.CalledProcessError as e:
        print("\n" + "=" * 60)
        print(f"❌ Tests failed with exit code: {e.returncode}")
        return False
    except FileNotFoundError:
        print("\n" + "=" * 60)
        print("❌ pytest not found. Please install it with: pip install pytest")
        return False

def main():
    parser = argparse.ArgumentParser(description="Run tests for autoloader-framework-pydab")
    parser.add_argument("--verbose", "-v", action="store_true", help="Run tests with verbose output")
    parser.add_argument("--coverage", "-c", action="store_true", help="Run tests with coverage report")
    parser.add_argument("--test", "-t", help="Run specific test file (e.g., test_expression_parser.py)")
    
    args = parser.parse_args()
    
    success = run_tests(verbose=args.verbose, coverage=args.coverage, specific_test=args.test)
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
