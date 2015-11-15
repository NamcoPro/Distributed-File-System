###############################################################################
#
# Filename: mds_db.py
# Author: Jose R. Ortiz and ... (hopefully some students contribution)
#
# Description:
# 	Copy client for the DFS
#
#
import sys
import socket
import sys
import os.path
from Packet import *

def usage():
    print """Usage:\n\tFrom DFS: python %s <server>:<port>:<dfs file path> <destination file>\n\tTo   DFS: python %s <source file> <server>:<port>:<dfs file path>""" % (sys.argv[0], sys.argv[0])
    sys.exit(0)

#This will be used to put the given file into "blocks"
def partition_string(string, p_size):
    """Returns a list of substrings of size p_size from the given string.
       In the case where the given size isn't a divisor of the original
       string's length, the very last file will have the remainder."""
    string_list = []
    if(len(string) < p_size):
        string_list.append(string)
        return string_list

    index_limit = len(string) / p_size
    for i in range(index_limit):
        #this partitions the string with the desired partition size
        string_list.append(string[(i * p_size):((i + 1) * p_size)])

    if(len(string) % p_size == 0):
        return string_list

    else:
        #the case where there's a remainder
        string_list.append(string[(index_limit * p_size):len(string)])
        return string_list

def usage():
	print """Usage: python %s <server>:<port, default=8000>""" % argv[0]
	exit(0)

#NEEDLESS OVERHEAD
#MUH SIZE DOESN'T FIT ALL
def recv_with_size(sock):
    """Receives a size so that the message can be sent in one go"""
    size = sock.recv(1024)

    sock.sendall("OK")

    message = sock.recv(int(size))

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
    sendall_with_size(sock, p.getEncodedPacket())

    # If no error or file exists
    # Get the list of data nodes.
    # Divide the file in blocks
    # Send the blocks to the data servers

    message = recv_with_size(sock)
    #print message
    p.DecodePacket(message)

    #Packet.getDataNodes() returns a list of elements
    #They are of the form (address, port)
    data_nodes = p.getDataNodes()
    node_amount = len(data_nodes) # would be nice if I implemented threads

    # blocks of size about 64k
    block_size = 2 ** 16
    #for files bigger than 100MB
    if(filesize > (100 * (10 ** 6))):
        block_size = 2 ** 22 #blocks of size about 4096K

    blocks = [] #for the metadata server

    #this divides the file into "blocks"
    file_segments = partition_string(file_string, block_size)
    #for iterating the nodes in a round robin style
    index = 0

    for segment in file_segments:
        #connecting to the data nodes
        node_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        IP = data_nodes[index % node_amount][0]
        PORT = data_nodes[index % node_amount][1]
        node_sock.connect((IP, PORT))

        #sends the put message to the current data node
        p.BuildPutPacket(fname, len(segment))
        sendall_with_size(node_sock, p.getEncodedPacket())

        #waiting for an OK
        OK = recv_with_size(node_sock)

        #sending the block to the data node
        sendall_with_size(node_sock, segment)

        #receive the unique block ID
        blockid = recv_with_size(node_sock)

        #adding muh blocks
        blocks.append((IP, str(PORT), blockid))

        index += 1

        node_sock.close()

    # Notify the metadata server where the blocks are saved.

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(address)
    #print blocks
    p.BuildDataBlockPacket(fname, blocks)
    sendall_with_size(sock, p.getEncodedPacket())
    meta_message = recv_with_size(sock)

    if(meta_message == "ACK"):
        print "Acknowledged."

    else:
        print "Something happened."
        print meta_message

    sock.close()

def copyFromDFS(address, fname, path):
    """ Contact the metadata server to ask for the file blocks of
        the file fname.  Get the data blocks from the data nodes.
        Saves the data in path.
    """
    # Contact the metadata server to ask for information of fname

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(address)
    p = Packet()
    p.BuildGetPacket(fname)
    sendall_with_size(sock, p.getEncodedPacket())

    # If there is no error response Retreive the data blocks

    # Save the file

    message = recv_with_size(sock)
    p.DecodePacket(message)
    #getDataNodes has ADDRESS, IP, BLOCK_ID
    data_nodes = p.getDataNodes()

    destination = "%s/%s" % (path, "copy_" + fname)
    wfile = open(destination, "w")
    for IP, PORT, BLOCK_ID in data_nodes:
        node_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        node_sock.connect((IP, PORT))

        p.BuildGetDataBlockPacket(BLOCK_ID)
        sendall_with_size(node_sock, p.getEncodedPacket())

        block = recv_with_size(node_sock)

        wfile.write(block)

        node_sock.close()

    wfile.close()

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

        copyFromDFS((ip, port), to_path, from_path)

    elif len(file_to) > 2:
        ip = file_to[0]
        port = int(file_to[1])
        to_path = file_to[2]
        from_path = sys.argv[1]

        if os.path.isdir(from_path):
            print "Error: path %s is a directory.  Please name the file." % from_path
            usage()

        copyToDFS((ip, port), from_path, to_path)
