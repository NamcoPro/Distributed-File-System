###############################################################################
#
# Filename: mds_db.py
# Author: Jose R. Ortiz and ... (hopefully some students contribution)
#
# Description:
# 	List client for the DFS
#
import socket
from sys import argv, exit

from Packet import *

def usage():
	print """Usage: python %s <server>:<port, default=8000>""" % argv[0]
	exit(0)

# Contacts the metadata server and ask for list of files.
def client(ip, port):
	#socket for connecting to the metadata server
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.connect((ip, port))

	#packet object used to interact with the metadata server
	p = Packet()
	#list command
	p.BuildListPacket()
	#sending the encoded list command
	sock.sendall(p.getEncodedPacket())

	#dealing with the metadata server's response
	message = sock.recv(1024)
	p.DecodePacket(message)

	#get file array is a cool guy and has all the files if successful
	for filename, size in p.getFileArray():
		print filename, size, "bytes"

	sock.close()

#what the fuck is going on
if __name__ == "__main__":

	if len(argv) < 2:
		usage()

	ip = None
	port = None
	server = argv[1].split(":")
	if len(server) == 1:
		ip = server[0]
		port = 8000
	elif len(server) == 2:
		ip = server[0]
		port = int(server[1])

	if not ip:
		usage()

	client(ip, port)
