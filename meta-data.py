###############################################################################
#
# Filename: meta-data.py
# Author: Jose R. Ortiz and ... (hopefully some students contribution)
#
# Description:
# 	MySQL support library for the DFS project. Database info for the
#       metadata server.
#
# Please modify globals with appropiate info.

from mds_db import *
from Packet import *
import sys
import SocketServer

def usage():
    print """Usage: python %s <port, default=8000>""" % sys.argv[0]
    sys.exit(0)


class MetadataTCPHandler(SocketServer.BaseRequestHandler):

    def handle_reg(self, db, p):
        """Register a new client to the DFS  ACK if successfully REGISTERED
            NAK if problem, DUP if the IP and port already registered
        """
        try:
            #new data node client, Packets and Databases man how do they work
            if(db.AddDataNode(p.getAddr(), p.getPort())):
                self.request.sendall("ACK")
            else:
                print "Duplicate register."
                self.request.sendall("DUP")
        except:
            print "Problem in handle_reg()."
            self.request.sendall("NAK")

    def handle_list(self, db):
        """Get the file list from the database and send list to client"""
        try:
            #Packet for the ls.py response
            list_p = Packet()
            #GetFiles() already sends the files in the neat format
            list_p.BuildListResponse(db.GetFiles())
            self.request.sendall(list_p.getEncodedPacket())

        except:
            print "Problem in handle_list()."
            self.request.sendall("NAK")

    def handle_put(self, db, p):
        """Insert new file into the database and send data nodes to save
           the file.
        """
        info = p.getFileInfo()

        #insert file makes node attributes
        if db.InsertFile(info[0], info[1]):
            #BuildPutResponse requires a metadata list,
            #getDataNodes returns a list of metadata
            p.BuildPutResponse(db.GetDataNodes())
            self.request.sendall(p.getEncodedPacket())

        else:
            print "Duplicate file."
            self.request.sendall("DUP")

    def handle_get(self, db, p):
        """Check if file is in database and return list of
            server nodes that contain the file.
        """

        # Fill code to get the file name from packet and then
        # get the fsize and array of metadata server

        #GetFileInode returns filesize and a list of the metadata
        filename = p.getFileName()
        fsize, meta_list = db.GetFileInode(filename)

        if fsize:
            #same as the others, making a packet for the response
            p.BuildGetResponse(meta_list, fsize)
            self.request.sendall(p.getEncodedPacket())
        else:
            print "handle_get() got 0 file size."
            self.request.sendall("NFOUND")

    #Doubts, I think they're cleared.
    #No, they're not.
    #Yes, they are.
    def handle_blocks(self, db, p):
        """Add the data blocks to the file inode"""
        # Fill code to get file name and blocks from
        # packet
        filename = p.getFileName()
        blocks = p.getDataBlocks()

        # Fill code to add blocks to file inode
        if(db.AddBlockToInode(filename, blocks)):
            self.request.sendall("ACK")

        else:
            print "Adding blocks failed."
            self.request.sendall("Adding blocks failed.")

    def handle(self):

        # Establish a connection with the local database
        db = mds_db("dfs.db")
        db.Connect()

        # Define a packet object to decode packet messages
        p = Packet()

        # Receive a msg from the list, data-node, or copy clients
        msg = self.request.recv(1024)
        print msg, type(msg)

        # Decode the packet received
        p.DecodePacket(msg)

        # Extract the command part of the received packet
        cmd = p.getCommand()

        # Invoke the proper action
        if   cmd == "reg":
            # Registration client
            self.handle_reg(db, p)

        elif cmd == "list":
            # Client asking for a list of files
            self.handle_list(db)

        elif cmd == "put":
            # Client asking for servers to put data
            self.handle_put(db, p)

        elif cmd == "get":
            # Client asking for servers to get data
            self.handle_get(db, p)

        elif cmd == "dblks":
            # Client sending data blocks for file
            self.handle_blocks(db, p)

        db.Close()

if __name__ == "__main__":
    HOST, PORT = "", 8000

    if len(sys.argv) > 1:
        try:
            PORT = int(sys.argv[1])
        except:
            usage()

    server = SocketServer.TCPServer((HOST, PORT), MetadataTCPHandler)

    # Activate the server; this will keep running until you
    # interrupt the program with Ctrl-C
    server.serve_forever()
