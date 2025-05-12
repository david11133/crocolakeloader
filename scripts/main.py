#!/usr/bin/env python3

## @file main.py
#
#
## @author Enrico Milanese <enrico.milanese@whoi.edu>
#
## @date Tue 03 Sep 2024

##########################################################################
import sys
import argparse
import importlib.metadata
##########################################################################

def main():

    # Ensure stdout and stderr are unbuffered
    sys.stdout = open(sys.stdout.fileno(), 'w', buffering=1)
    sys.stderr = open(sys.stderr.fileno(), 'w', buffering=1)

    parser = argparse.ArgumentParser(description='Package load CrocoLake database.')
    parser.add_argument('--version', action='store_true', help="Show version and exit")

    args = parser.parse_args()

    if args.version:
        try:
            version = importlib.metadata.version('crocolake-loader')
            print(f"crocolake-loader version: {version}")
        except importlib.metadata.PackageNotFoundError:
            print("crocolake-loader version: not installed")

    else:
        parser.print_help()

##########################################################################

if __name__ == "__main__":
    main()
