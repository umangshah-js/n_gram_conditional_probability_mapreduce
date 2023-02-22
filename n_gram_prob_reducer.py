#!/usr/bin/env python
"""An advanced Reducer, using Python iterators and generators."""

from itertools import groupby
from operator import itemgetter
import sys


# receive the output of a mapper, (key, [value, value, ...])
def read_mapper_output(input, separator='\t'):
    for line in input:
        #  return each (key, [value, value, ...]) tuple, though there should only be one per line
        yield line.rstrip().split(separator)


def main(separator='\t'):
    # input comes from STDIN (standard input)
    data = read_mapper_output(sys.stdin, separator=separator)

    # groupby groups multiple word-count pairs by word
    # and creates an iterator that returns consecutive keys and their group:
    #   current_word - string containing a word (the key)
    #   group - iterator yielding all ["&lt;current_word&gt;", "&lt;count&gt;"] items
    last = None
    denominator = 0
    for fields in data:
        key = fields[0]
        fields = fields[1:]
        print >> sys.stderr,fields
        try:
            if fields[0] == "-":
                # if last != None:
                #     print("%s%s%d" % (last, separator, count))
                # print(key,fields)
                last = key
                denominator = float(fields[-1])
                # print(fields[0],denominator)
            else:
                words = fields[:-1]
                numerator = float(fields[-1][1:])
                print('%s%s%s%s%.20f' % (key," "," ".join(words), separator, numerator/float(denominator)))
            
        except ValueError:
            # count was not a number, so silently discard this item
            pass
if __name__ == "__main__":
    main()