#!/usr/bin/perl
use strict;
use warnings;
use 5.010;

my %hash_ip_exists;
my %hash_ip_used;

open(IP_V4,'../data/SIC1.ic_ip_v4.unl')||die  "error can not open IP_ACC_NBR.unl";
open(UPDATE_SQL,'>../data/update.sql')||die "error can not create update.sql";
open(IP_INSERT,'>../data/IC_ACC_IP.unl')||die "error can not create ic_acc_ip.unl";

foreach(1..4){

    open(IP_USED,'../data/CC'.$_.'.subs_upp_inst_value.unl')||die "error cannot open subs_upp_inst_value";
    while(<IP_USED>){
        chomp();
        my @arr=split(/\|/,$_);
        if(exists $hash_ip_exists{$arr[2]}){
            print('update ic_apn_ip set state=\'C\' where IP=\''.$arr[2].'\';');
        }
        $hash_ip_used{$arr[2]}='A';

    }
    close(IP_USED);
}

while(<IP_V4>){
    chomp();
    my @arr=split(/\|/,$_);
    if(exists $hash_ip_used{$arr[3]}){
        say UPDATE_SQL 'update ic_ip_v4 set state=\'C\' where ip_v4_id='.$arr[2]
    }
}
close(IP_V4);