import os
import sys
import subprocess

def check_dbt_environment():
    # Check if a virtual environment is activated
    if not hasattr(sys, 'real_prefix') and not sys.base_prefix != sys.prefix:
        print("Warning: No virtual environment is currently activated.")
        return False

    # Check if dbt is installed
    try:
        subprocess.run(["dbt", "--version"], check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError:
        print("Error: dbt is not installed or not in the system PATH.")
        return False
    except FileNotFoundError:
        print("Error: dbt command not found. Make sure dbt is installed and in your PATH.")
        return False

    # Check for specific environment variables (customize as needed)
    required_vars = ['DBT_PROFILES_DIR', 'DBT_PROJECT_DIR']
    for var in required_vars:
        if var not in os.environ:
            print(f"Warning: Environment variable {var} is not set.")
            return False

    print("dbt environment check passed successfully.")
    return True

if __name__ == "__main__":
    if not check_dbt_environment():
        sys.exit(1)
    else:
        sys.exit(0)