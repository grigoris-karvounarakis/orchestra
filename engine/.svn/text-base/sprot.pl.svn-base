#!/usr/bin/perl

# A script to read in uniprot_sprot.dat from SwissProt and put
# selected fields of interest into files.

use strict;

my %func;

open(ENTRIES, ">entries.txt");

my $os = "";
my $id;
my %refs;

while (<>) {
	if (/^ID   ([A-Z0-9]{1,5}_[A-Z0-9]{1,5})/) {
		$id = $1;
	} elsif (/^ID  /) {
	} elsif (/^OS   (.+)\n$/) {
		$os .= $1;
	} elsif (/^CC   -!- FUNCTION: (.+)\./) {
		++$func{$1};
	} elsif (/^DR   ([^;]+); ([^;]+)/) {
		$refs{$1} = $2;
	} elsif (/^\/\/$/) {
		chop($os);
		$os = substr($os,0,120);
		print ENTRIES $id, "\t", $os;
		foreach (sort keys(%refs)) {
			print ENTRIES "\t", $_, "\t", substr($refs{$_}, 0, 50);
		}
		print ENTRIES "\n";
		$os = "";
		undef %refs;
	}
}

my @ids;
my @org;
my @func;

my $key;

foreach $key (keys(%func)) {
	push(@func, {val => $key, count => $func{$key}});
}

open(FUNC,">func.txt");

my $rec;

foreach $rec (sort byCount @func) {
	printf FUNC "%s\t%s\n", $rec->{'val'}, $rec->{'count'};
}

close(FUNC);
close(ENTRIES);

sub byCount {
	return $b->{'count'} <=> $a->{'count'};
}
