#!/usr/bin/env python3
import sys

from collections import defaultdict

word_counts = defaultdict(int)

for line in sys.stdin:
    word, count = line.split('\t', 1)
    try:
        word_counts[word] += int(count)
    except ValueError:
        continue

for word, count in word_counts.items():
    print(f'{word}\t{count}')
