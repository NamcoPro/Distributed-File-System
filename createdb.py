import sqlite3

conn = sqlite3.connect("dfs.db") 

c = conn.cursor()

# Create inode table 
c.execute("""CREATE TABLE inode (fid INTEGER PRIMARY KEY ASC AUTOINCREMENT, fname TEXT UNIQUE NOT NULL DEFAULT " ", fsize INTEGER NOT NULL default "0")""")

# Create data node table
c.execute("""CREATE TABLE dnode(nid INTEGER PRIMARY KEY ASC AUTOINCREMENT, address TEXT NOT NULL default " ", port INTEGER NOT NULL DEFAULT "0")""") 

# Create UNIQUE tuple for data node
c.execute("""CREATE UNIQUE INDEX dnodeA ON dnode(address, port)""")

# Create block table 
c.execute("""CREATE TABLE block (bid INTEGER PRIMARY KEY ASC AUTOINCREMENT, fid INTEGER NOT NULL DEFAULT "0", nid INTEGER NOT NULL DEFAULT "0", cid TEXT NOT NULL DEFAULT "0")""")

# Create UNIQUE tuple for block
c.execute("""CREATE UNIQUE INDEX blocknc ON block(nid, cid)""") 
