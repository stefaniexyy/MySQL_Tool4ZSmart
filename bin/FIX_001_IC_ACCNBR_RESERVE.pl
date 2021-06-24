#!/usr/bin/perl
use strict;
use warnings;
use 5.010;

my @ic_acc_nbr_reserve;

$ic_acc_nbr_reserve[0]='\\N'; #ACC_NBR_ID
$ic_acc_nbr_reserve[1]=1; #SEQ=""
$ic_acc_nbr_reserve[2]='\\N'; #CREATED_DATE
$ic_acc_nbr_reserve[3]='A'; #STATE
$ic_acc_nbr_reserve[4]='\\N'; #STATE_DATE
$ic_acc_nbr_reserve[5]='S'; #RESERVE_TYPE
$ic_acc_nbr_reserve[6]='\\N'; #CERT_TYPE_ID
$ic_acc_nbr_reserve[7]='\\N'; #CERT_NBR
$ic_acc_nbr_reserve[8]='\\N'; #EXP_DATE
$ic_acc_nbr_reserve[9]='\\N'; #PWD
$ic_acc_nbr_reserve[10]='\\N'; #RESERVE_DATE
$ic_acc_nbr_reserve[11]='2099-01-01 00:00:00'; #RESERVE_EXP_DATE
$ic_acc_nbr_reserve[12]='F'; #PARTY_TYPE
$ic_acc_nbr_reserve[13]='1'; #PARTY_CODE
$ic_acc_nbr_reserve[14]='\\N'; #RESERVE_PARTY_TYPE
$ic_acc_nbr_reserve[15]='\\N'; #RESERVE_PARTY_CODE
$ic_acc_nbr_reserve[16]='\\N'; #RESERVE_COMMENTS
$ic_acc_nbr_reserve[17]='\\N'; #CANCEL_DATE
$ic_acc_nbr_reserve[18]='\\N'; #CANCEL_PARTY_TYPE
$ic_acc_nbr_reserve[19]='\\N'; #CANCEL_PARTY_CODE
$ic_acc_nbr_reserve[20]='\\N'; #CUST_TYPE

foreach my $i(1..4){
    open(FILE,'../data/SIC'.$i.'.ic_acc_nbr.unl')||die "error can not open xxxx";
    open(FILE_OUT,'>../data/SIC'.$i.'.ic_acc_nbr_reserve.unl')||die "error can not open xxxx";
    while(<FILE>){
        chomp();
        my @arr=split(/\|/,$_);
        $ic_acc_nbr_reserve[0]=$arr[0+2];
        $ic_acc_nbr_reserve[2]=$arr[13+2];
        $ic_acc_nbr_reserve[4]=$arr[13+2];
        $ic_acc_nbr_reserve[10]=$arr[13+2];
        say FILE_OUT join('|',@ic_acc_nbr_reserve);
    }
    close(FILE);
    close(FILE_OUT);
}