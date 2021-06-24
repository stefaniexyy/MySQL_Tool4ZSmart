#!/usr/bin/perl
use strict;
use warnings;
use 5.010;

my $in_file='../data/'.$ARGV[0];
my ($out_file)=($ARGV[0])=~/^([A-Za-z1-9_]+).tmp$/;
$out_file='../data/'.$out_file.'.unl';
open(IN,$in_file)||die "error can not open $in_file";
open(OUT,'>'.$out_file)||die "error can not open $in_file";

while(<IN>){
    chomp();
    my @arr=split(/\|/,$_,-1);
    for(my $i=0;$i<scalar(@arr)-1;$i++){
        $arr[$i]=$arr[$i] eq ""?("\\N"):($arr[$i]);
    }
    say OUT join('|',@arr[0..scalar(@arr)-2]);

}
