#!/bin/bash
clear
./btree_init mydisk 64 6 6
./btree_insert mydisk 64 a 15
./btree_insert mydisk 64 b 4
./btree_insert mydisk 64 c 8
./btree_insert mydisk 64 d 6
./btree_insert mydisk 64 e 5
./btree_insert mydisk 64 f 9
./btree_insert mydisk 64 g 4
./btree_insert mydisk 64 h 12
./btree_insert mydisk 64 i 15
./btree_insert mydisk 64 j 12
./btree_insert mydisk 64 k 19
./btree_insert mydisk 64 l 19
./btree_display mydisk 64 normal