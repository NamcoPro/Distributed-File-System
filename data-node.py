###############################################################################
#
# Filename: data-node.py
# Author: Jose R. Ortiz and ... (hopefully some students contribution)
#
# Description:
# 	data node server for the DFS
#

from Packet import *

import sys
import socket
import SocketServer
import uuid
import os.path

def usage():
    print """Usage: python %s <server> <port> <data path> \
<metadata port,default=8000>""" % sys.argv[0]
    sys.exit(0)

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

def register(meta_ip, meta_port, data_ip, data_port):
    """Creates a connection with the metadata server and
       register as data node
    """

    # Establish connection
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((meta_ip, meta_port))

    #Wow. Everything was all here. Fucking Packets, man.
    try:
        response = "NAK"
        sp = Packet()
        while response == "NAK":
            sp.BuildRegPacket(data_ip, data_port)
            sendall_with_size(sock, sp.getEncodedPacket())
            response = recv_with_size(sock)

            if response == "DUP":
                print "Duplicate Registration"

            if response == "NAK":
                print "Registratation ERROR"

    finally:
        sock.close()


class DataNodeTCPHandler(SocketServer.BaseRequestHandler):

    def recv_with_size(self):
        """Receives a size so that the message can be sent in one go"""
        size = self.request.recv(1024)

        self.request.sendall("OK")

        message = self.request.recv(1024)
        while(len(message) < int(size)):
            message += self.request.recv(1024)

        return message

    def sendall_with_size(self, message):
        """Sends a size for the message so that the receiver can receive in
        one go."""

        self.request.sendall(str(len(message)))

        OK = self.request.recv(1024)

        if(OK == "OK"):
            self.request.sendall(message)

        else:
            print "sendall_with_size had a problem with %s." % message

    def handle_put(self, p):
        """Receives a block of data from a copy client, and
           saves it with an unique ID.  The ID is sent back to the
           copy client.
        """

        fname, fsize = p.getFileInfo()
        #print fsize, type(fsize)

        self.sendall_with_size("OK")

        # Generates an unique block id.
        blockid = str(uuid.uuid1())

        # Open the file for the new data block.
        path_and_name = "%s/%s" % (DATA_PATH, blockid)
        block_file = open(path_and_name, "w")

        # Receive the data block.
        block = self.recv_with_size()

        #writting the block contents to the file
        block_file.write(block)
        block_file.close()

        # Send the block id back
        self.sendall_with_size(blockid)

    def handle_get(self, p):

        # Get the block id from the packet
        blockid = p.getBlockID()

        # Read the file with the block id data
        # Send it back to the copy client.
        path_and_name = "%s/%s" % (DATA_PATH, blockid)
        block_file = open(path_and_name, "r")

        block = block_file.read()
        block_file.close()

        self.sendall_with_size(block)

    def handle(self):
        msg = self.recv_with_size()
        print msg, type(msg)

        p = Packet()
        p.DecodePacket(msg)

        cmd = p.getCommand()
        if cmd == "put":
            self.handle_put(p)

        elif cmd == "get":
            self.handle_get(p)


if __name__ == "__main__":

    META_PORT = 8000
    if len(sys.argv) < 4:
        usage()

    try:
        HOST = sys.argv[1]
        PORT = int(sys.argv[2])
        DATA_PATH = sys.argv[3]

        if len(sys.argv) > 4:
            META_PORT = int(sys.argv[4])

        if not os.path.isdir(DATA_PATH):
            print "Error: Data path %s is not a directory." % DATA_PATH
            usage()
    except:
        usage()


    register("localhost", META_PORT, HOST, PORT)
    server = SocketServer.TCPServer((HOST, PORT), DataNodeTCPHandler)

    # Activate the server; this will keep running until you
    # interrupt the program with Ctrl-C
    server.serve_forever()
