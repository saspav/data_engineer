#!/usr/bin/env python3
import re
import sys

for line in map(str.strip, sys.stdin):
    for char in '",()':
        line = line.replace(char, ' ')
    for word in line.split():
        print(f'{word}\t1')
