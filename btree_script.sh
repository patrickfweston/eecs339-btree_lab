#!/bin/bash
./btree_init mydisk 64 6 6
./btree_insert mydisk 64 3 4
./btree_insert mydisk 64 1 4
./btree_insert mydisk 64 2 6
./btree_insert mydisk 64 0 8
./btree_insert mydisk 64 7 5
./btree_insert mydisk 64 6 9
./btree_insert mydisk 64 12 12
./btree_display mydisk 64 dot
./btree_insert mydisk 64 13 15
#./btree_display mydisk 64 dot