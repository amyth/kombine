#!/usr/bin/env python

import sys
from kombine import Kombiner


def usage():
    print "\nUSAGE:"
    print "python kombine_postfix.py /path/to/input/file\n"

if __name__ == '__main__':

    if len(sys.argv) < 2:
        usage()
        sys.exit(0)

    source = sys.argv[1]

    try:
        sys.stdout.write("Kombiner Started. Now kombining postfix logs from %s"%
                source)
        kombiner = Kombiner(source)
        kombiner.kombine()
    except KeyboardInterrupt:
        sys.stdout.write("Kombiner terminated.")
    except Exception as err:
        print str(err)
