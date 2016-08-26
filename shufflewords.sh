#!/bin/bash

cat $1 | awk 'BEGIN{srand();}{print rand()"\t"$0}' | sort -k1 -n | cut -f2- | head -n 1000 > words.shuffled.txt
