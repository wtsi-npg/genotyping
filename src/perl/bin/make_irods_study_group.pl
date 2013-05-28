#!/software/bin/perl

use utf8;

package main;

use strict;
use warnings;
use AnyEvent;
use Getopt::Long;
use JSON;
use Log::Log4perl qw(:easy);
use Net::RabbitFoot;
use Pod::Usage;

use WTSI::NPG::iRODS qw(group_exists add_group);

$| = 1;


my $embedded_conf = q(
   log4perl.logger.verbose        = DEBUG, A1
   log4perl.logger.quiet          = DEBUG, A2

   log4perl.appender.A1          = Log::Log4perl::Appender::Screen
   log4perl.appender.A1.stderr   = 0
   log4perl.appender.A1.layout   = Log::Log4perl::Layout::PatternLayout
   log4perl.appender.A1.layout.ConversionPattern = %d %p %m %n

   log4perl.appender.A2          = Log::Log4perl::Appender::Screen
   log4perl.appender.A2.stderr   = 0
   log4perl.appender.A2.layout   = Log::Log4perl::Layout::PatternLayout
   log4perl.appender.A2.layout.ConversionPattern = %d %p %m %n
   log4perl.appender.A2.Filter   = F2

   log4perl.filter.F2               = Log::Log4perl::Filter::LevelRange
   log4perl.filter.F2.LevelMin      = WARN
   log4perl.filter.F2.LevelMax      = FATAL
   log4perl.filter.F2.AcceptOnMatch = true
);


run() unless caller();

sub run {
  my $host;
  my $log4perl_config;
  my $password;
  my $path;
  my $port;
  my $prefetch;
  my $user;
  my $verbose;

  GetOptions('help'       => sub { pod2usage(-verbose => 2, -exitval => 0) },
             'host=s'     => \$host,
             'logconf=s'  => \$log4perl_config,
             'password=s' => \$password,
             'port=s'     => \$port,
             'prefetch=i' => \$prefetch,
             'user=s'     => \$user,
             'verbose'    => \$verbose,
             'path=s'     => \$path);

  unless (defined $user) {
    pod2usage(-msg => "A --user argument is required\n", -exitval => 2);
  }
  unless (defined $password) {
    $password = '';
  }

  $host     ||= 'localhost';
  $port     ||= 5672;
  $prefetch ||= 10;
  $path     ||= 'production';

  my $log;

  if ($log4perl_config) {
    Log::Log4perl::init(\$log4perl_config);
    $log = Log::Log4perl->get_logger('npg.irods.studies');
  }
  else {
    Log::Log4perl::init(\$embedded_conf);
    if ($verbose) {
      $log = Log::Log4perl->get_logger('verbose');
    }
    else {
      $log = Log::Log4perl->get_logger('quiet');
    }
  }

  my $connection = Net::RabbitFoot->new()->load_xml_spec()->connect
    (host    => $host,
     port    => $port,
     user    => $user,
     pass    => $password,
     timeout => 1,
     vhost   => $path);

  my $channel = $connection->open_channel;

  # Avoid fetching the entire queue
  $channel->qos(prefetch_count => $prefetch);

  # Create an iRODS group and ack when done
  $channel->consume(queue => 'npg.irods.studies',
                    on_consume => sub {
                      my $frame = shift;
                      my $payload = $frame->{body}->payload;
                      my $tag = $frame->{deliver}->method_frame->delivery_tag;
                      $log->debug("Received message $tag");

                      if (find_or_create_group($payload, $log)) {
                        $log->debug("Created a new iRODS group; acking message $tag");
                        $channel->ack(delivery_tag => $tag);
                      }
                      else {
                        $log->error("Failed to find or create a group; not acking message $tag");
                      }
                    },
                    no_ack => 0);

  # Set up event loop that will eventually quit
  my $quit = AnyEvent->condvar;

  # Break out of event loop cleanly on these signals
  my $sigint;
  $sigint = AnyEvent->signal(signal => "INT",
                             cb => sub {
                               $log->info("Got SIGINT, quitting ...");
                               $quit->send;
                               undef $sigint;
                             });
  my $sigterm;
  $sigterm = AnyEvent->signal(signal => "TERM",
                              cb => sub {
                                $log->info("Got SIGTERM, quitting ...");
                                $quit->send;
                                undef $sigterm;
                              });

  # Wait until told to quit
  $quit->recv;
}

sub make_group_name {
  my ($study_id) = @_;

  return "ss_" . $study_id;
}

sub find_or_create_group {
  my ($json, $log) = @_;

  my $message = from_json($json, {utf8 => 1});

  # The message is structured
  # {"study": {"id": <study id> ... }}

  my $study_id = $message->{'study'}->{'id'};
  my $group_name = make_group_name($study_id);
  my $group;

  $log->debug("Received notification of study $study_id");

  if (group_exists($group_name)) {
    $group = $group_name;
    $log->debug("An iRODS group '$group' exists; a new group will not be added");
  }
  else {
    $group = add_group($group_name);
    $log->info("Added a new iRODS group '$group' in response to study $study_id");
  }

  return $group;
}


__END__

=head1 NAME

make_irods_study_group

=head1 SYNOPSIS

make_irods_study_group --host <host> [--port <port>] [--path <path>] \
   --user <user> --password <password> [--prefetch <n>] [--verbose] \
   [--logconf <path>]

Options:

  --help           Display help.
  --host           The RabbitMQ queue host. Optional, defaults
                   to 'localhost'.
  --logconf        A log4perl configuration file. Optional.
  --path           The RabbitMQ URL path on the host. Optional, defaults
                   to 'production'.
  --password       The RabbitMQ password.
  --port           The RabbitMQ port. Optional, defaults to 5672.
  --prefetch       The number of messages to prefetch. Optional, defaults
                   to 10.
  --user           The RabbitMQ user.
  --verbose        Print messages while processing. Optional.


=head1 DESCRIPTION

Listens to the RabbitMQ message queue npg.irods.studies for messages
broadcast by SequenceScape when new studies are created. Creates a new
iRODS group for each study. The scipt will ack the message only after
the group has been created. If the named group exists already, it will
be skipped.

Once launched, the scipt will sit in an event loop, waiting for
messages. SIGINT or SIGTERM will interrupt the event loop cleanly and
cause the script to exit.


=head1 METHODS

None

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (c) 2012 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
