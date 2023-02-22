#!/usr/bin/env python
"""An advanced Mapper, using Python iterators and generators."""

import sys
import re

def read_input(input):
    for line in input:
        # split the line into words; keep returning each word
        # line = re.sub(r"[^a-z0-9 ]","",line.lower())
        yield line.strip()


def main(total,separator='\t'):
    # input comes from STDIN (standard input)
    data = read_input(sys.stdin)

    for line in data:
        # write the results to STDOUT (standard output);
        # what we output here will be the input for the
        # Reduce step, i.e. the input for reducer.py
        #
        # tab-delimited; the trivial word count is 1
        n_gram,count = line.split(separator)
        print('%s%s%.20f' % (n_gram, separator, float(count)/total))
        


# how to test locally in bash/linus: cat <input> | python mapper.py
if __name__ == "__main__":
    total = int(sys.argv[1])
    main(total)
