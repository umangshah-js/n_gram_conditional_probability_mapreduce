#!/usr/bin/env python
"""Python Reducer/Combiner to sum all n_gram counts

Sample Command:
mapred streaming -file n_gram_count_mapper.py \
-file n_gram_sum_reducer.py \
-input 1_grams/* \
-output 1_grams_sum \
-mapper "cat" \ #Equivalent to using UnitMapper
-combiner "python n_gram_sum_reducer.py" \
-numReduceTasks 1 \ #not necessary since only one key "sum" is returned from mapper/combiner
-reducer "python n_gram_sum_reducer.py"
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
    data = read_mapper_output(sys.stdin, separator=separator)
    count = 0
    for current_word, current_count in data:
        try:
            count+= int(current_count)
        except ValueError:
            # count was not a number, so silently discard this item
            pass
    print("%s%s%d" % ("Sum", separator, count))


if __name__ == "__main__":
    main()
