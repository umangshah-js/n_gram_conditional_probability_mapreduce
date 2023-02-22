#!/usr/bin/env python
"""Python Mapper to calculate n_gram
Sample command for unigram count

hadoop fs -rm -r 1_grams ; \
mapred streaming -file n_gram_count_mapper.py \
-file n_gram_count_reducer.py \
-input hw1.2/* \
-output 1_grams \
-mapper "python n_gram_count_mapper.py 1 3" \
-reducer "python n_gram_count_reducer.py"
"""

import sys
import re

def read_input(input,min_words):
    for line in input:
        # split the line into words; keep returning each word
        #replace everything except alpha numerric with empty string (replceing with space might casue issues in corner cases)
        line = re.sub(r"[^a-z0-9 ]"," ",line.lower())
        line = re.sub(r"(\s+)|(\t)"," ",line) # replace more than one space with single space. replace tabs with space
        splits = line.split()
        if len(splits)>=min_words:
            yield line.split()


def main(n,min_words,separator='\t'):
    # input comes from STDIN (standard input)
    data = read_input(sys.stdin,min_words)

    for words in data:
        # write the results to STDOUT (standard output);
        # what we output here will be the input for the
        # Reduce step, i.e. the input for reducer.py
        #
        # tab-delimited; the trivial word count is 1
        n_gram = []
        for word in words:
            n_gram.append(word)
            if len(n_gram) == n:
               print('%s%s%d' % (" ".join(n_gram), separator, 1))
               n_gram.pop(0)
        


# how to test locally in bash/linus: cat <input> | python mapper.py
if __name__ == "__main__":
    n = int(sys.argv[1]) # n for n_gram, n =2 for bigram
    min_words = int(sys.argv[2]) # minimum words in a sentence to consider a line as valid
    main(n,min_words)
