# dd-analysis


This package contains of two main parts:

## ./watchers ##
A set of scripts which should be run in a .god process to handle file processing for newly uploaded datasets 

## ./server ##
Flask server to handle queries from the client machine, including sliced datasets and file exports

## NOTES ##
POSTGRES SETUP
Installation of this app requires postgres to store geneid queries etc. We set up postgres as follows:

`
sudo apt-get install postgresql postgresql-contrib
conda install psycopg
sudo -u postgres psql
create database dd;
create user ben_coolship_io;
grant all privileges on database dd to ben_coolship_io;
ALTER USER ben_coolship_io WITH password  'password';
`