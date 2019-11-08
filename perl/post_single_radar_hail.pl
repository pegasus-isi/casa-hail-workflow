#!/usr/bin/perl -w

##################################################################################################
#####WRITTEN BY ERIC LYONS 3/2018 for CASA, UNIVERSITY OF MASSACHUSETTS##########################
##################################################################################################
#  TESTED FUNCTIONALITY:         
#  Monitors nowcast directory
#  pqinserts nowcast files
#  plots nowcast files
#  pqinserts nowcast images
# 
#  #                                                                                                  #
##################################################################################################

use POSIX qw(setsid);
use File::Copy;
use File::Monitor;

use threads;
use threads::shared;

our $input_data_dir;
our @cart_input_files;
##Parse Command Line
&command_line_parse;

&daemonize;

##Realtime Mode -- Gets MCC stream
my $file_mon = new threads \&file_monitor;

sleep 900000000;

sub file_monitor {
    
    my $dir_monitor = File::Monitor->new();
        
    $dir_monitor->watch( {
	name        => "$input_data_dir",
	recurse     => 1,
        callback    => \&new_files,
    } );
    
    $dir_monitor->scan;
    
    for ($i=0; $i < 9000000000; $i++) {
	my @changes = $dir_monitor->scan;   
	sleep 3;
    }
    
    sub new_files 
    {
	my ($name, $event, $change) = @_;
	@new_netcdf_files = $change->files_created;
	my @dels = $change->files_deleted;
	print "Added: ".join("\nAdded: ", @new_netcdf_files)."\n" if @new_netcdf_files;
	foreach $file (@new_netcdf_files) {
	    my $pathstr;
            my $filename;
            ($pathstr, $filename) = $file =~ m|^(.*[/\\])([^/\\]+?)$|;
            my $suffix = substr($filename, -3, 3);
	    
	    if ($suffix eq "cdf") {
		print $filename . "\n";
		$nexrad = index ($filename, 'burleson.tx');
		if ($nexrad ne -1) {
		    #print "this is nexrad data\n"; 
		    my $ymdhmstr = substr($filename, -30, 13);
		    foreach $pmatch (glob "$input_data_dir/*$ymdhmstr*cdf") {
			#print "pmatch: " . $pmatch . "\n";
			($thispathstr, $thisfilename) = $pmatch =~ m|^(.*[/\\])([^/\\]+?)$|;
			my $indirpath = "/home/ldm/hailworkflow/input/" . $thisfilename;
			copy($pmatch, $indirpath);
			push @cart_input_files, $thisfilename;
			unlink($pmatch);
		    }
		    my $wfdir = $ENV{'HAIL_WORKFLOW_DIR'};
		    system("$wfdir/run_composite_wf.sh @cart_input_files");
		    @cart_input_files = ();   
		}
		else {
		    #print "this is casa data\n";
		}
	    }
	    elsif ($suffix eq "png") {
		my $pngpqins = "pqinsert -f EXP -p " . $filename . " " . $file;
		system($pngpqins);
		sleep 1;
		unlink $file;
	    }
	    elsif ($suffix eq "son") {
		my $jsonpqins = "pqinsert -f EXP -p " . $filename . " " . $file;
		system($jsonpqins);
		sleep 1;
		unlink $file;
	    }
	    else {
		print $filename . "\n";
	    }
	}
    }
}

sub daemonize {
    chdir '/'                 or die "Can't chdir to /: $!";
    open STDIN, '/dev/null'   or die "Can't read /dev/null: $!";
    open STDOUT, '>>/dev/null' or die "Can't write to /dev/null: $!";
    open STDERR, '>>/dev/null' or die "Can't write to /dev/null: $!";
    defined(my $pid = fork)   or die "Can't fork: $!";
    exit if $pid;
    setsid                    or die "Can't start a new session: $!";
    umask 0;
}

sub command_line_parse {
    if ($#ARGV < 0) { 
	print "Usage:  dir_mon.pl netcdf_dir\n";
   	exit; 
    }
    $input_data_dir = $ARGV[0];
    
    my @rdd = split(/ /, $input_data_dir);
    foreach $w (@rdd) {
	print "Will recursively monitor $w for incoming netcdf files\n";
    }
    
}
