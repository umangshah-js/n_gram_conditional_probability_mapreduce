#!/usr/bin/env python
"""Python mapper to calculate conditional probability from n_gram counts and total count.

Sample Command:
mapred streaming \
-D stream.num.map.output.key.fields=2 \ # tell hadoop that key is comprised of two fields
-D mapred.text.key.partitioner.options="-k1,1" \ # partition only on first field.
-D mapred.text.key.comparator.options="-k1,2" \ #ensures n-1 gram (the) is always above the n grams(the cat) count
-file n_gram_prob_mapper.py \
-file n_gram_prob_reducer.py \
-input 2_grams/* \
-input 1_grams_prob/* \
-output 2_grams_prob \
-mapper "python n_gram_prob_mapper.py 2 $(hadoop fs -cat 2_grams_sum\/*  | cut -f 2)" \
-reducer "python n_gram_prob_reducer.py" \
--partitioner "org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner"
"""

import sys
import re

def read_input(input):
    for line in input:
        # split the line into words; keep returning each word
        # line = re.sub(r"[^a-z0-9 ]","",line.lower())
        yield line.strip()


def main(n,total,separator='\t'):
    # input comes from STDIN (standard input)
    data = read_input(sys.stdin)

    for line in data:
        n_gram,count = line.split(separator)
        n_gram_splits = n_gram.split(" ")

        # print <n-1 words><tab><nth word><tab><probability> since n-1 words will be used as key in next step.
        # %.20f for probability to deal with very small values. 
        if len(n_gram_splits)==n:
            print("%s%s%s%s%.20f" % (" ".join(n_gram_splits[:-1]),separator, n_gram_splits[-1],separator, float(count)/total))
        else:# n-1 gram word for conditional probability
            print('%s%s%s%s%s' % (n_gram, separator, "-", separator, count))
        


# how to test locally in bash/linus: cat <input> | python mapper.py
if __name__ == "__main__":
    n = int(sys.argv[1]) # n =2 for bigram
    total = int(sys.argv[2]) #Total no of n_grams (To be used in denominator)
    main(n,total)
