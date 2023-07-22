#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    words = line.replace('"', ' " ').replace(',', ' ').split()
    for word in words:
        print(f'{word}\t1')
