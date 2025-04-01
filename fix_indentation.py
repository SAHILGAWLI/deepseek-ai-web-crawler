#!/usr/bin/env python3

with open('devpost_crawler.py', 'r') as f:
    lines = f.readlines()

# Fix indentation for lines 1133-1135 (0-indexed would be 1132-1134)
for i in range(1132, 1135):
    if i < len(lines):
        # Add proper indentation (24 spaces)
        lines[i] = ' ' * 24 + lines[i].lstrip()

with open('devpost_crawler.py', 'w') as f:
    f.writelines(lines)

print('Indentation fixed in devpost_crawler.py!') 