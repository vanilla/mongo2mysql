# mongo2mysql

This tool is a general purpose command line utility that will take a mongoDB and convert it to a mysql database.
Right now this is mostly the case, but I ran into the clown car that is the nodeBB database structure and had to hard
code in some kludges for its schema. Perhaps some dedicated data export specialist can refactor those kludges in the future.

## Features

This is a fairly basic tool, but there are some features of note.

* Creates the mysql schema and imports the data automatically from a given mongoDB.
* Infers database types from the values in rows, automatically making schema changes as necessary.
* Flattens deeply nested object structures.
* Creates child tables from array values and ginormous child objects.
* Reasonable percent complete and time estimation so you don't pull your hair out wondering what's taking so long.

## Installation

You are going to need php, composer, mysql, mongoDB, and the php-mongo extension to use this tool.

1. Clone this repository locally.
2. Run `composer install` to bring in dependencies.
3. Symlink `bin/mongo2mysql` into a directory in your path. Protip: Make yourself a `~/bin` directory for all your little
command line tools.
4. Run `mongo2mysql --help` to see what to do next.

## Restoring a MongoDB

Generally, you'll need to restore a mongoDB database to the same computer as your mysql database.
To do this you can use the [mongorestore](//docs.mongodb.org/manual/tutorial/backup-with-mongodump/#restore-a-database-with-mongorestore) command line utility.

1. Unzip the bson files in the backup.
2. The directory of the bson files will become the name of the mongoDB so rename it if you want to.
3. Call `mongorestore dirname/` to restore the files.


## Limitations

Since mongoDB is schemaless it really can give developers enough rope to hang themselves with.
As such some databases may not export properly or at all. Here are some things to keep in mind.

* Rows with two many keys won't be exported. You'll see a message printed when a row is skipped including that row's _id
attribute so you can look up the row and see what's wrong.

* The type checking isn't exhaustive at this point, but new types can be added easily.

* Indexes aren't transferred. Make sure you add necessary indexes before processing resulting data.

* The export does a schema check on every row of every mongoDB collection which is a way of saying this code isn't really
optimized. The tool was implemented against a database with about 8 million rows and the export takes about an hour.