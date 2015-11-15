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

def recv_with_size(sock):
    """Receives a size so that the message can be sent in one go"""
    size = sock.recv(1024)

    sock.sendall("OK")

    message = sock.recv(1024)
    while(len(message) < int(size)):
        message += sock.recv(1024)

    return message

def sendall_with_size(sock, message):
    """Sends a size for the message so that the receiver can receive in
    one go."""
    sock.sendall(str(len(message)))

    OK = sock.recv(1024)

    if(OK == "OK"):
        sock.sendall(message)

    else:
        print "sendall_with_size had a problem with %s." % message

# Contacts the metadata server and ask for list of files.
def client(ip, port):
    #socket for connecting to the metadata server
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((ip, port))

    #packet object used to interact with the metadata server
    p = Packet()
    #list command
    p.BuildListPacket()

    #sending the size and later the command
    message = p.getEncodedPacket()
    sendall_with_size(sock, message)

    #dealing with the metadata server's response
    response = recv_with_size(sock)
    p.DecodePacket(response)

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
