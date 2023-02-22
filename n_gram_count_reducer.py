#!/usr/bin/env python
"""Python Reducer to calculate n_gram
Sample command for unigram count

hadoop fs -rm -r 1_grams ; \
mapred streaming -file n_gram_count_mapper.py \
-file n_gram_count_reducer.py \
-input hw1.2/* \
-output 1_grams \
-mapper "python n_gram_count_mapper.py 1 3" \
-reducer "python n_gram_count_reducer.py"
"""

from itertools import groupby
from operator import itemgetter
import sys


# receive the output of a mapper, (key, [value, value, ...])
def read_mapper_output(input, separator='\t'):
    for line in input:
        #  return each (key, [value, value, ...]) tuple, though there should only be one per line
        yield line.rstrip().split(separator, 1)


def main(separator='\t'):
    # input comes from STDIN (standard input)
    # detects keys,values as they come and update the count until same key in encountered
    # print last key with count when new keys are encountered
    data = read_mapper_output(sys.stdin, separator=separator)
    last = None
    count = 0
    for current_word, current_count in data:
        try:
            if current_word != last:
                if last != None:
                    print("%s%s%d" % (last, separator, count))
                last = current_word
                count = int(current_count)
            else:
                count+= int(current_count)
            
        except ValueError:
            # count was not a number, so silently discard this item
            pass
    if last != None:
        print("%s%s%d" % (last, separator, count))
        # print the last key before EOF


if __name__ == "__main__":
    main()
