#!/usr/bin/env python3
import sys
from collections import defaultdict

word_counts = defaultdict(int)
for line in data:
    word, count = line.rsplit('\t', 1)
    try:
        word_counts[word] += int(count)
    except ValueError:
        pass

for word, count in sorted(word_counts.items(), key=lambda x: (-x[1], x[0])):
    print(f'{word}\t{count}')