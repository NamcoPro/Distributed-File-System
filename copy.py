###############################################################################
#
# Filename: mds_db.py
# Author: Jose R. Ortiz and ... (hopefully some students contribution)
#
# Description:
# 	Copy client for the DFS
#
#


import socket
import sys
import os.path
from Packet import *

def usage():
    print """Usage:\n\tFrom DFS: python %s <server>:<port>:<dfs file path> <destination file>\n\tTo   DFS: python %s <source file> <server>:<port>:<dfs file path>""" % (sys.argv[0], sys.argv[0])
    sys.exit(0)

def copyToDFS(address, fname, path):
    """ Contact the metadata server to ask to copy file fname,
        get a list of data nodes. Open the file in path to read,
        divide in blocks and send to the data nodes.
    """

    # Create a connection to the data server

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(address)

    # Read file

    rfile = open(fname, "r")
    file_string = rfile.read()
    filesize = len(file_string)
    rfile.close()

    # Create a Put packet with the fname and the length of the data,
    # and sends it to the metadata server

    p = Packet()
    p.BuildPutPacket(fname, filesize)
    sock.sendall(p.getEncodedPacket())

    # If no error or file exists
    # Get the list of data nodes.
    # Divide the file in blocks
    # Send the blocks to the data servers

    message = sock.recv(1024)
    print message
    p.DecodePacket(message)
    #Packet.getDataNodes() returns a list of elements
    data_nodes = p.getDataNodes()
    node_amount = len(data_nodes) #would be nice if I implemented threads
    block_size = 1024
    blocks = []

    #each data node will have its own list of blocks where
    #each block has an address, port, and an ID
    for i in range(node_amount - 1):
        segment = file_string[(i * block_size):((i + 1) * block_size)]
        p.BuildPutPacket(filename, block_size)





    # Notify the metadata server where the blocks are saved.

    # Fill code

#Doubts
def copyFromDFS(address, fname, path):
    pass
    """ Contact the metadata server to ask for the file blocks of
        the file fname.  Get the data blocks from the data nodes.
        Saves the data in path.
    """

       # Contact the metadata server to ask for information of fname

    #getting the information given the file's filename

    #receiving response


    # If there is no error response Retreive the data blocks


    # Fill code

        # Save the file

    # Fill code

if __name__ == "__main__":
#	client("localhost", 8000)
    if len(sys.argv) < 3:
        usage()

    file_from = sys.argv[1].split(":")
    file_to = sys.argv[2].split(":")

    if len(file_from) > 1:
        ip = file_from[0]
        port = int(file_from[1])
        from_path = file_from[2]
        to_path = sys.argv[2]

        if os.path.isdir(to_path):
            print "Error: path %s is a directory.  Please name the file." % to_path
            usage()

        copyFromDFS((ip, port), from_path, to_path)

    elif len(file_to) > 2:
        ip = file_to[0]
        port = int(file_to[1])
        to_path = file_to[2]
        from_path = sys.argv[1]

        if os.path.isdir(from_path):
            print "Error: path %s is a directory.  Please name the file." % from_path
            usage()

        copyToDFS((ip, port), from_path, to_path)
