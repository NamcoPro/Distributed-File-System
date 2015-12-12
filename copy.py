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
from simplecrypt import encrypt, decrypt

def usage():
    #it looks ugly here, it's much better on the shell.
    print """Usage:\nFrom DFS: python %s <server>:<port>:<dfs file path> \
<destination file> <decrypting key, default=secret:^)>\nTo DFS: python %s \
 <source file> <server>:<port>:<dfs file path>\
 <encrypting key, default=secret:^)>""" % (sys.argv[0], sys.argv[0])
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

def copyToDFS(address, dfs_path, filename, password, crypto):
    """ Contact the metadata server to ask to copy file fname,
        get a list of data nodes. Open the file in path to read,
        divide in blocks and send to the data nodes.
    """

    # Create a connection to the data server

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(address)

    #get filesize

    filesize = os.path.getsize(filename)

    # Create a Put packet with the filename and the length of the data,
    # and sends it to the metadata server

    p = Packet()
    p.BuildPutPacket(dfs_path, filesize)
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
    node_amount = len(data_nodes)

    #for distributing the workload
    if(filesize / node_amount == 0):
        partition_size = filesize
    else:
        partition_size = filesize / node_amount

    #blocks of about 4K
    block_size = 2 ** 12
    if(filesize > 40 * (10 ** 6)):
        #blocks of size about 4096K
        block_size = 2 ** 22

    blocks = [] #for the metadata server

    rfile = open(filename, "r")
    for IP, PORT in data_nodes:
        temp_load = partition_size
        while(temp_load):
            node_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            node_sock.connect((IP, PORT))

            if(temp_load < block_size):
                segment = rfile.read(temp_load)
                temp_load = 0
            else:
                segment = rfile.read(block_size)
                temp_load -= block_size

            #variable from the main function to indicate if crypto is needed
            if(crypto):
                crpt_seg = encrypt(password, segment)
            else:
                crpt_seg = segment

            #sending a put message to the data node
            p.BuildPutPacket(dfs_path, len(crpt_seg))
            sendall_with_size(node_sock, p.getEncodedPacket())

            #the OK message
            OK = recv_with_size(node_sock)

            #sends the block to the data node
            sendall_with_size(node_sock, crpt_seg)

            #receive the unique block ID
            blockid = recv_with_size(node_sock)

            #adding muh blocks
            blocks.append((IP, str(PORT), blockid))

            node_sock.close()

    #not everything was cleared, the last node gets the load
    #almost the same code as above
    if(filesize % node_amount != 0):
        while(1):
            node_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            node_sock.connect((data_nodes[-1][0], data_nodes[-1][1]))

            segment = rfile.read(block_size)
            #encountered an end of file
            if(segment == ""):
                break

            if(crypto):
                crpt_seg = encrypt(password, segment)
            else:
                crpt_seg = segment

            #sending a put message to the data node
            p.BuildPutPacket(dfs_path, len(crpt_seg))
            sendall_with_size(node_sock, p.getEncodedPacket())

            #the OK message
            OK = recv_with_size(node_sock)

            #sends the block to the data node
            sendall_with_size(node_sock, crpt_seg)

            #receive the unique block ID
            blockid = recv_with_size(node_sock)

            #adding muh blocks
            blocks.append((IP, str(PORT), blockid))

            node_sock.close()

    rfile.close()

    # Notify the metadata server where the blocks are saved.

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(address)
    p.BuildDataBlockPacket(dfs_path, blocks)
    sendall_with_size(sock, p.getEncodedPacket())
    meta_message = recv_with_size(sock)

    if(meta_message == "ACK"):
        #print "Acknowledged."
        pass
    else:
        print "Something happened."
        print meta_message

    sock.close()

def copyFromDFS(address, dfs_path, filename, password, crypto):
    """ Contact the metadata server to ask for the file blocks of
        the file fname.  Get the data blocks from the data nodes.
        Saves the data in path.
    """
    # Contact the metadata server to ask for information of fname

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(address)
    p = Packet()
    p.BuildGetPacket(dfs_path)
    sendall_with_size(sock, p.getEncodedPacket())

    # If there is no error response Retreive the data blocks

    # Save the file

    message = recv_with_size(sock)
    p.DecodePacket(message)
    #getDataNodes has ADDRESS, PORT, BLOCK_ID
    data_nodes = p.getDataNodes()

    """Not needed."""
    #In case the user specifies a diferent directory

    #filename = fname.split("/")

    #destination = "%s/%s" % (path, "copy_" + filename[-1])
    """Not needed."""

    wfile = open(filename, "w")
    for IP, PORT, BLOCK_ID in data_nodes:
        node_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        node_sock.connect((IP, PORT))

        p.BuildGetDataBlockPacket(BLOCK_ID)
        sendall_with_size(node_sock, p.getEncodedPacket())

        block = recv_with_size(node_sock)

        #added for decrypting
        if(crypto):
            decrpt_block = decrypt(password, block)

        else:
            decrpt_block = block

        wfile.write(decrpt_block)

        node_sock.close()

    wfile.close()

if __name__ == "__main__":
#	client("localhost", 8000)
    if len(sys.argv) < 3:
        usage()

    file_from = sys.argv[1].split(":")
    file_to = sys.argv[2].split(":")
    password = "password"   #maximum security, unbreakable
    crypto = 0

    #gets a file from the arguments and uses its contents as a password
    if(len(sys.argv) == 4):
        rfile = open(sys.argv[3], "r")
        password = rfile.read()
        rfile.close()
        crypto = 1

    if len(file_from) > 1:
        ip = file_from[0]
        port = int(file_from[1])
        from_path = file_from[2]
        to_path = sys.argv[2]

        if os.path.isdir(to_path):
            print "Error: path %s is a directory.  Please name the file." % to_path
            usage()

        copyFromDFS((ip, port), from_path, to_path, password, crypto)

    elif len(file_to) > 2:
        ip = file_to[0]
        port = int(file_to[1])
        to_path = file_to[2]
        from_path = sys.argv[1]

        if os.path.isdir(from_path):
            print "Error: path %s is a directory.  Please name the file." % from_path
            usage()

        copyToDFS((ip, port), to_path, from_path, password, crypto)
