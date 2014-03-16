#!/bin/bash
clear
./deletedisk mydisk
./makedisk mydisk 1024 128 1 16 64 100 10 .28
./btree_init mydisk 64 3 6
./btree_insert mydisk 64 aaa 15
./btree_insert mydisk 64 baa 4
./btree_insert mydisk 64 caa 8
./btree_insert mydisk 64 aha 12
./btree_insert mydisk 64 daa 6
./btree_insert mydisk 64 eaa 5
./btree_insert mydisk 64 faa 9
./btree_insert mydisk 64 gaa 4
./btree_insert mydisk 64 haa 12
./btree_insert mydisk 64 iaa 15
./btree_insert mydisk 64 aja 12
./btree_insert mydisk 64 jaa 12
./btree_insert mydisk 64 kaa 19
./btree_insert mydisk 64 laa 19
./btree_insert mydisk 64 afa 9
./btree_insert mydisk 64 aga 4
 ./btree_insert mydisk 64 aia 15
 ./btree_insert mydisk 64 aka 19
 ./btree_insert mydisk 64 ala 19
 ./btree_insert mydisk 64 cja 12
./btree_insert mydisk 64 cka 19
./btree_insert mydisk 64 tja 12
./btree_insert mydisk 64 tka 19
./btree_insert mydisk 64 tla 19
./btree_insert mydisk 64 bja 12
./btree_insert mydisk 64 bka 19
./btree_insert mydisk 64 bla 19
./btree_insert mydisk 64 dab 19
./btree_insert mydisk 64 dac 19
./btree_insert mydisk 64 dad 19
./btree_insert mydisk 64 dae 19
./btree_insert mydisk 64 daf 19
./btree_insert mydisk 64 dag 19
./btree_insert mydisk 64 bbp 19
./btree_insert mydisk 64 bbo 19
./btree_insert mydisk 64 bbt 19
./btree_insert mydisk 64 bbq 19
./btree_insert mydisk 64 bbs 19
./btree_insert mydisk 64 bbe 19
./btree_insert mydisk 64 bbz 19
./btree_insert mydisk 64 bcb 1
./btree_insert mydisk 64 bza 2
./btree_insert mydisk 64 bjz 3
./btree_insert mydisk 64 bkz 19
./btree_insert mydisk 64 blz 19
./btree_insert mydisk 64 aaf 9
./btree_insert mydisk 64 aag 4
./btree_insert mydisk 64 ajk 15
./btree_insert mydisk 64 ajl 15
./btree_insert mydisk 64 xjm 15
./btree_insert mydisk 64 xak 19
./btree_insert mydisk 64 xal 19
./btree_insert mydisk 64 xcj 12
./btree_insert mydisk 64 xcm 19
./btree_insert mydisk 64 xcn 19
./btree_insert mydisk 64 xao 19
./btree_insert mydisk 64 xap 19
./btree_insert mydisk 64 xcq 12
./btree_insert mydisk 64 xcr 19
 ./btree_insert mydisk 64 zao 19
 ./btree_insert mydisk 64 zap 19
 ./btree_insert mydisk 64 zcq 12
# ./btree_insert mydisk 64 zcr 19
# ./btree_insert mydisk 64 zzo 19
# ./btree_insert mydisk 64 zzp 19
# ./btree_insert mydisk 64 zzq 19
# ./btree_insert mydisk 64 zzr 19
#./btree_insert mydisk 64 wap 19
#./btree_insert mydisk 64 wcq 12
#./btree_insert mydisk 64 wcr 19
./btree_display mydisk 64 normal
