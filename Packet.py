###############################################################################
#
# Filename: Packet.py
# Author: Jose R. Ortiz and ... (hopefully some students contribution)
#
# Description:
# 	Packet creation support library for the DFS project. Database info for
#
# Please modify globals with appropiate info.

import json

class Packet:

	def __init__(self):
	
		self.commands = ["reg", "list", "put", "get", "dblks"]
		self.packet = {}
		
	def getEncodedPacket(self):
		"""returns a seriliazed packet ready to send through the network.  
		First you need to build the packets.  See BuildXPacket functions."""

		return json.dumps(self.packet) 

	def getCommand(self):
		"""Returns the command type of a packet"""

		if self.packet.has_key("command"):
			return self.packet["command"]
		return None

	def getAddr(self):
		"""Returns the IP address of a server""" 
		if self.packet.has_key("addr"):
			return self.packet["addr"]
		return None
	def getPort(self):
		"""Returns the port number of a server"""
		if self.packet.has_key("port"):
			return self.packet["port"]
		return None

	def DecodePacket(self, packet):
		"""Receives a serialized message and turns it into a packet object."""
		self.packet = json.loads(packet)	

	def BuildRegPacket(self, addr, port):
		"""Builds a registration packet"""
		self.packet = {"command": "reg", "addr": addr, "port": port}
		

	def BuildListPacket(self):
		"""Builds a list packet for file listing"""

		self.BuildCommand("list")

	def BuildListResponse(self, lfiles):
		"""Builds a list response packet"""

		self.packet = {"files": lfiles}	

	def getFileArray(self):
		"""Builds a list response packet"""

		if self.packet.has_key("files"):
			return self.packet["files"]


	def BuildGetPacket(self, fname):
		"""Build a get packet to get fname."""
		self.BuildCommand("get")
		self.packet["fname"] = fname

	def BuildPutPacket(self, fname, fsize):
		"""Builds a put packet to put fname and file size."""
		self.BuildCommand("put")
		self.packet["fname"] = fname
		self.packet["fsize"] = fsize

	def BuildDataBlockPacket(self, fname, block_list):
		"""Builds a data block packet. Contains the file name and the list of blocks for the file"""
		self.BuildCommand("dblks")
		self.packet["blocks"] = block_list
		self.packet["fname"] = fname

	def BuildGetDataBlockPacket(self, blockid):
		"""Builds a get data block packet. Usefull when requesting a data block to a data node."""

		self.BuildCommand("get")
		self.packet["blockid"] = blockid

	def getBlockID(self):
		"""Returns a the block_id from a packet."""
			return self.packet["blockid"]

	def getFileInfo(self):
		"""Returns the file info in a packet."""
		if self.packet.has_key("fname") and self.packet.has_key("fsize"):
			return self.packet["fname"], self.packet["fsize"] 

	def getFileName(self):
		"""Returns the file name in a packet."""
		if self.packet.has_key("fname"):
			return self.packet["fname"] 

	def BuildGetResponse(self, metalist, fsize):
		"""Builds a list of data node servers with the blocks of a file, and file size."""
		self.packet["servers"] = metalist
		self.packet["fsize"] = fsize

	def BuildPutResponse(self, metalist):
		"""Builds a list of data node servers where a file data blocks can be stored.
		I.E. a list of available data servers."""
		self.packet["servers"] = metalist

	def getDataNodes(self):
		"""Returns a list of data servers"""
		if self.packet.has_key("servers"):
			return self.packet["servers"]
		return None

	def getDataBlocks(self):
		"""Returns a list of data blocks""" 
		if self.packet.has_key("blocks"):
			return self.packet["blocks"]
		return None

	def BuildCommand(self, cmd):
		"""Builds a packet type"""
		if cmd in self.commands:
			self.packet = {"command": cmd}



		
	
		
		
