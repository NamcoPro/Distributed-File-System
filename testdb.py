###############################################################################
#
# Filename: test.py
# Author: Jose R. Ortiz and ... (hopefully some students contribution)
#
# Description:
#       Script to test the MySQL support library for the DFS project.
#
#

# This is how to import a local library
from mds_db import *

# Create an object of type mds_db
db = mds_db("dfs.db") 

# Connect to the database
print "Connecting to database" 
db.Connect() 

# Testing how to add a new node to the metadata server.
# Note that I used a node name, the address and the port.
# Address and port are necessary for connection.

print "Testing node addition"
id1 = db.AddDataNode("136.145.54.10", 80) 
id2 = db.AddDataNode("136.145.54.11", 80) 
print 
print "Testing if node was inserted"
print "A tupple with node name and connection info must appear"
print db.CheckNode(id1)
print

print "Testing all Available data nodes"
for address, port in  db.GetDataNodes():
	print address, port

print 

print "Inserting two files to DB"
db.InsertFile("/hola/cheo.txt", 20)
db.InsertFile("/opt/blah.txt", 30)
print

print "Choteando one of the steps of the assignment :) ..."
print "Files in the database"
for file, size in db.GetFiles():
	print file, size
print

print "Adding blocks to the file, duplicate message if not the first time running"
print "this script"
try:
	db.AddBlockToInode("/hola/cheo.txt", [(id1, "1"), (id2, "1")])
except:
	print "Won't duplicate"
print

print "Testing retreiving Inode info"
fsize, chunks_info = db.GetFileInode("/hola/cheo.txt")

print "File Size is:", fsize
print "and can be constructed from: "
for  address, port, chunk in chunks_info:
	print address, port, chunk
print

print "Closing connection"
db.Close() 
