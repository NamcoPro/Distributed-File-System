###############################################################################
#
# Filename: mds_db.py
# Author: Jose R. Ortiz and ... (hopefully some students contribution)
#
# Description:
# 	MySQL support library for the DFS project. Database info for the 
#       metadata server.
#

import sqlite3

class mds_db:

	def __init__(self, db_name):
		self.c = None
		self.db_name=db_name
		self.conn = None
	
	def Connect(self):
		"""Connect to the database file"""
		try:
			self.conn = sqlite3.connect(self.db_name)
			self.c = self.conn.cursor()
			self.conn.isolation_level = None
			return 1
		except:
			return 0

	def Close(self):
		"""Close cursor to the database"""
		try:
			#self.conn.commit()
			self.c.close() 	
			return 1
		except:
			return 0
	
	def AddDataNode(self, address, port):
		"""Adds new data node to the metadata server
		   Receives IP address and port 
		   I.E. the information to connect to the data node
		"""
          
		query = """insert into dnode (address, port) values ("%s", %s)""" % (address, port)
		try:
			self.c.execute(query)
			return self.c.lastrowid 
		except sqlite3.IntegrityError as e: 
			#print type(e), dir(e), e
			if e.message.split()[-1].strip() == "unique":
				return 0
			else:
				raise
			
	def CheckNode(self, address, port):
		"""Check if node is in database and returns name, address, port
                   for connection.
		"""
		query = """select nid from dnode where address="%s" and port=%s""" % (address, port)
		try:
			self.c.execute(query)
		except:
			return None
		return self.c.fetchone()[0]

	def GetDataNodes(self):
		"""Returns a list of data node tuples (address, port).  Usefull to know to which 
		   datanodes chunks can be send.
		"""

		query = """select address, port from dnode where 1"""
		self.c.execute(query)
		return self.c.fetchall()

	def InsertFile(self, fname, fsize):
		"""Create the inode attributes.  For this project the name of the
		   file and its size.
		"""
		query = """insert into inode (fname, fsize) values ("%s", %s)""" % (fname, fsize)
		try:
			self.c.execute(query)
			return 1
		except:
			return 0
	
 	def GetFileInfo(self, fname):
		"""Given a filename, if the file is stored in DFS
     		   return its filename id and fsize.  Internal use only.
		   Does not have to be accessed from the metadata server.
		"""
		query = """select fid, fsize from inode where fname="%s" """ % fname
		try:
			self.c.execute(query)
			result = self.c.fetchone()
			return result[0], result[1]
		except:
			return None, None

	def GetFiles(self):
		"""Returns the attributes of the files stored in the DFS"""
		"""File Name and Size"""

		query = """select fname, fsize from inode where 1""" ;
		self.c.execute(query)	
		return self.c.fetchall()

	def AddBlockToInode(self, fname, blocks):
		"""Once the Inode was created with the file's attribute
  	           and the data copied to the data nodes.  The inode is 
		   updated to point to the data blocks. So this function receives
                   the filename and a list of tuples with (node id, chunk id)
		"""
		fid, dummy1= self.GetFileInfo(fname) 
		if not fid:
			return None
		for address, port, chunkid in blocks:
			nid = self.CheckNode(address, port)
			if nid:
				query = """insert into block (nid, fid, cid) values (%s, %s, "%s")""" % (nid, fid, chunkid)
				self.c.execute(query)
			else:
				return 0 
		return 1

	def GetFileInode(self, fname):
		"""Knowing the file name this function return the whole Inode information
	           I.E. Attributes and the list of data blocks with all the information to access 
                   the blocks (node name, address, port, and the chunk of the file).
		"""

		fid, fsize = self.GetFileInfo(fname)
		if not fid:
			return None, None
		query = """select address, port, cid from dnode, block where dnode.nid = block.nid and block.fid=%s""" % fid
		self.c.execute(query)
		return fsize, self.c.fetchall() 
		
