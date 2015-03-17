#!/usr/bin/perl

my $VERSION = 1.0;

use constant ;
use Data::Dumper;
use strict ;
use DBI qw(:sql_types);	# pour gérer SQLite
use Time::HiRes qw(time); # convert sec, min, hour, day, mounth, year into sec since 1970
use Data::Uniqid qw(luniqid);
use File::Copy; # move
use Getopt::Long ;

my ($sql,$dont_create,$delimiter,$debug,$help);
$delimiter = ';';
GetOptions('sql=s'=>\$sql, 'dont-create!'=>\$dont_create , 'debug!'=>\$debug, 'delimiter=s'=>\$delimiter, 'help|usage!'=>\$help) ;

my $start = time ; #for stats
my $sqlite;	# sqlite object
my $filename; #name of csv file and sqlite file
my @columns_type; # type of the sqlite columns
my @values_to_transform ; # for data manipulation (date-fr for example)

# print usage
print_usage();

# trouve le nom du fichier CSV dans la requete SQL
find_filename();

# creer le squellete de la base sql en fonction des noms des champs
create_sqlite() if !$dont_create;

# charge les données depuis le fichier csv
load_data_from_csv() if !$dont_create;

# interroge la base SQLite
ask_database();


###########################################################################################################################################
# find_filename
###########################################################################################################################################
sub find_filename() {
	# verifie que l'on a une requete SQL
	die "Error : SQL request empty" if length($sql)<=0;

	$sql =~ m/\s+from\s+([^\s]+)\s?/i;
	$filename = $1;

	die "Error : Unable to determine CSV file in FROM clause" if length($filename)<=0;
	die "Error : Unable to find file' $filename'" if !-e $filename;

	if ($dont_create) {
		$sqlite = DBI->connect("dbi:SQLite:$filename.sqlite",'','',{ RaiseError => 0, AutoCommit => 0 }) or die "Unable to create new SQLite database '$filename.sqlite'";	
	}

}


###########################################################################################################################################
# create_sqlite
###########################################################################################################################################
sub create_sqlite() {
	if (-e "$filename.sqlite") {
		print "Erase old SQLite database '$filename.sqlite'\n" if $debug;
		unlink("$filename.sqlite") or die "Unable to delete old SQLite database '$filename.sqlite'";
	}

	# creation de la nouvelle base sqlite
	print "Create new SQLite database '$filename.sqlite'\n" if $debug;
	$sqlite = DBI->connect("dbi:SQLite:$filename.sqlite",'','',{ RaiseError => 0, AutoCommit => 0 }) or die "Unable to create new SQLite database '$filename.sqlite'";

	# on ouvre le CSV , on regarde les entete et l'on creer les colonnes
	open(CSV,"<$filename") or die "Unable to open CSV file '$filename' for reading ($!)";
	my $first_line = <CSV>;
	my $size_of_first_line = length($first_line); # pour faire un seek plus tard
	chomp($first_line);
	my @entetes = split(/$delimiter/,$first_line);

	# transforme les espace des nom de colonne en _
	# supprime les " en debut et fin de valeur
	# transforme les caracteres accentués
	# met les nom de colonnes en majuscule
	map { s/^"|"$//g; s/\s+/_/g; tr/éêèëàâäîìïùûüôöòÿ/eeeeaaaiiiuuuoooy/ ; $_ = uc ;} @entetes;

	# essai de déterminer le type de donnée dans les colonnes
	my $second_line = <CSV>; chomp($second_line);
	my @example_of_datas = split(/$delimiter/,$second_line);
	map { $_ = trim($_) ; } @example_of_datas;
	for(my $i=0 ; $i<=$#example_of_datas ; $i++) {
		my $type = dertermine_type_of_data($example_of_datas[$i]);
		if ($type eq 'date-fr') { # on garde en memoire que l'on devra transformer les dates-fr lors de l'importation de données
			push @values_to_transform, $i;
		}
		print "  Columns $i '".$entetes[$i]."' looks like a $type (example : '".$example_of_datas[$i]."')\n" if $debug;
		$columns_type[$i] = $type;
	}

	my @create_columns ;
	for(my $i=0 ; $i<=$#entetes ; $i++) {
		$create_columns[$i] = "[".$entetes[$i]."] ".($columns_type[$i] eq 'date-fr' ? 'text':$columns_type[$i])."\n";
	}

	my 	$create_table = "CREATE TABLE [$filename] (".
		join(',',@create_columns).
		");";

	$sqlite->do($create_table);
	$sqlite->commit;

	# on se replace juste apres les entetes pour traiter la 1ere ligne de données
	seek(CSV,$size_of_first_line + 1,0);
}


###########################################################################################################################################
# load_data_from_csv
###########################################################################################################################################
sub load_data_from_csv() {
	print "Inserting values in database... " if $debug;
	while(<CSV>) {
		chomp;
		my @values = split(/$delimiter/);
		
		#s'il manque des valeurs, on comble avec du vide
		if ($#values < $#columns_type) {
			for(my $i=0 ; $i<= $#columns_type - $#values ; $i++) {
				push @values, '';
			}
		}
		
		map { s/^"|"$//g; $_= trim($_) ; } @values;
		for(my $i=0 ; $i<=$#values_to_transform ; $i++) { # s'il y a des valeurs a transformer, on le fait
			my $type = $columns_type[$values_to_transform[$i]];

			if ($type eq 'date-fr') { # pour les date en format fr, on passe de dd/mm/yyyy en yyyy-mm-dd
				$values[$values_to_transform[$i]] = join('-',reverse(split(/\//,$values[$values_to_transform[$i]])));
			}
		}

		map { $_ = "'".quotify($_)."'" } @values; # double les ' si besoin
		my $sql_insert = "INSERT INTO [$filename] VALUES (".join(',',@values).");";
		$sqlite->do($sql_insert);
	}

	close CSV;
	$sqlite->commit;
	print "OK\n" if $debug;
}

###########################################################################################################################################
# ask_database
###########################################################################################################################################
sub ask_database() {
	print "Requesting database\n" if $debug;

	my 	$sql_request = $sql;
		$sql_request =~ s/\s+from\s+$filename/ FROM [$filename] /i;
	my 	$res = $sqlite->prepare($sql_request);
  		$res->execute;

  	# get request columns names
  	my @columns_name = @{$res->{NAME}};

  	# determine maximum size of the columns
  	my @size_of_columns;
  	for(my $i=0 ; $i<=$#columns_name ; $i++) {
  		$size_of_columns[$i] = length($columns_name[$i]);
  	}

 	my $all_results = $res->fetchall_arrayref;
 	foreach (@$all_results) {
  		for(my $i=0 ; $i<=$#columns_name ; $i++) {
	  		$size_of_columns[$i] = max($size_of_columns[$i],length($_->[$i]));
	  	}
  	}
	
	# print columns names
  	for(my $i=0 ; $i<=$#columns_name ; $i++) {
  		print $columns_name[$i]. (' ' x ($size_of_columns[$i] - length($columns_name[$i]))).' ';
  	}
  	print "\n";
  	for(my $i=0 ; $i<=$#columns_name ; $i++) {
  		print '-' x $size_of_columns[$i].' '; # print "-------------------"
  	}
  	print "\n";

  	# print results
  	foreach (@$all_results) {
  		for(my $i=0 ; $i<=$#columns_name ; $i++) {
	  		print $_->[$i]. (' ' x ($size_of_columns[$i] - length($_->[$i]))).' ';
	  	}
	  	print "\n";
  	}

	$sqlite->disconnect();
	print "\nRequest done in ".sprintf("%0.4f",time - $start)." sec\n";
}


###########################################################################################################################################
# print_usage
###########################################################################################################################################
sub print_usage() {
	die <<EOT if ($help);
Argument list :
--sql=select ... from csv_file.csv ...
--dont-create
	Do not recreate the sqlite database, only request the existing one
--delimiter=;
	Delimiter for CSV file. Default caracter is ;
--usage ou --help
	Display this message
--debug
	Display more information about the process
EOT
}


###########################################################################################################################################
# useful
###########################################################################################################################################
sub dertermine_type_of_data($) {
	my $data = shift;

	if 		($data =~ /^"?\d{2}\/\d{2}\/\d{4}"?$/) { # ca ressemble a un format de date type dd/mm/yyyy
			return 'date-fr';
	} elsif 	($data =~ /^"/ && $data =~ /"$/) { # " en debut et fin de valeur ---> text
			return 'text';
	} elsif ($data =~ /^(?:\-|\+)?\s*[\d ]+$/) { # type entier signé
			return 'integer';
	} elsif ($data =~ /^(?:\-|\+)?\s*[\d\.\, ]+$/) { # type real (float)
			return 'real';
	} else { # default
		return 'text';
	}
}

sub max($$) {
	my ($arg1,$arg2) = @_;
	if ($arg1 > $arg2) {
		return $arg1;
	}
	return $arg2;
}

sub trim {
	my $t = shift;
	$t =~ s/^\s+//g;
	$t =~ s/\s+$//g;
	return $t ;
}

sub quotify {
	my $t = shift;
	$t =~ s/'/''/g;
	return $t ;
}