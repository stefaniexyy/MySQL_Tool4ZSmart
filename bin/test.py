#!/usr/bin/py3
import curses
import time
import os
import random
from multiprocessing import Process,Pipe
import sys

def hash_code(s):
     h = 0
     if len(s) > 0:
         for item in s:
             h = 31 * h + ord(item)
         return h
     else:
         return 0

print(hash_code('90&10240164189')%4)

