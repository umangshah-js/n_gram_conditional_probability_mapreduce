#!/usr/bin/env python
import sys
import logging
import os

def read_inputs(input,separator):
    for line in input:
        yield line.strip().split(separator)

def main(separator="\t"):
    data = read_inputs(sys.stdin,separator)
    for fields in data:
        try:
            key = " ".join(fields[0].split(" ")[:-1])
            next_word = fields[0].split(" ")[-1]
            val = fields[-1]
            print("%s%s%s%s%s"%(key,separator,val,separator,next_word))
        except:
            pass

if __name__ == "__main__":
    main()
