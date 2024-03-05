import pickle, logging
import argparse
import time
#import fsconfig

from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler

import hashlib

#function to compute md5 checksum

def md5_checksum(data):
    
    md5 = hashlib.md5()
    md5.update(data)
    return md5.hexdigest()


# Restrict to a particular path.
class RequestHandler(SimpleXMLRPCRequestHandler):
  rpc_paths = ('/RPC2',)

class DiskBlocks():

  def __init__(self, total_num_blocks, block_size, delayat):
    # This class stores the raw block array
    self.block = []
    self.checksums = {}
    # initialize request counter
    self.counter = 0
    self.delayat = delayat
    # Initialize raw blocks
    for i in range (0, total_num_blocks):
      putdata = bytearray(block_size)
      self.block.insert(i,putdata)
      checksum = md5_checksum(putdata)
      self.checksums[i] = checksum
    
  def Sleep(self):
    self.counter += 1
    if (self.counter % self.delayat) == 0:
      time.sleep(10)

if __name__ == "__main__":

  # Construct the argument parser
  ap = argparse.ArgumentParser()

  ap.add_argument('-nb', '--total_num_blocks', type=int, help='an integer value')
  ap.add_argument('-bs', '--block_size', type=int, help='an integer value')
  ap.add_argument('-port', '--port', type=int, help='an integer value')
  ap.add_argument('-cblk','--corrupted_block',type = int,help='specific block in the block array an interger value')
  ap.add_argument('-delayat', '--delayat', type=int, help='an integer value')

  args = ap.parse_args()

  if args.total_num_blocks:
    TOTAL_NUM_BLOCKS = args.total_num_blocks
  else:
    print('Must specify total number of blocks')
    quit()

  if args.block_size:
    BLOCK_SIZE = args.block_size
  else:
    print('Must specify block size')
    quit()

  if args.port:
    PORT = args.port
  else:
    print('Must specify port number')
    quit()

  if args.delayat:
    delayat = args.delayat
  else:
    # initialize delayat with artificially large number
    delayat = 1000000000

  CORRUPTED_BLOCK = -1
  if args.corrupted_block is not None or args.corrupted_block == 0:
    CORRUPTED_BLOCK = args.corrupted_block
    print("CORRUPTED_BLOCK: " + str(CORRUPTED_BLOCK))

  # initialize blocks
  RawBlocks = DiskBlocks(TOTAL_NUM_BLOCKS, BLOCK_SIZE, delayat)

  #corrupted_data if corrupted_block
  temp_data = bytearray("BLOCK_CORRUPTED", "utf-8")
  CORRUPT_DATA = bytes(temp_data.ljust(BLOCK_SIZE,b'\x00'))

  temp_data = bytearray("BLOCK_CORRUPTED_SHOW_MESSAGE_BLOCK", "utf-8")
  CORRUPT_DATA_2 = bytes(temp_data.ljust(BLOCK_SIZE,b'\x00'))

  #calculating server hits
  SERVER_HITS = 0

  # Create server
  server = SimpleXMLRPCServer(("127.0.0.1", PORT), requestHandler=RequestHandler)


  def Get(block_number):

    global SERVER_HITS
    SERVER_HITS = SERVER_HITS + 1
    result = RawBlocks.block[block_number]
    checksum = md5_checksum(result)

    if checksum != RawBlocks.checksums.get(block_number,None):
      #print('the result value ',result)
      print("BLOCK_CORRUPTED :" +str(block_number))
      RawBlocks.Sleep()
      return -1
    
    RawBlocks.Sleep()
    return result

  server.register_function(Get)

  #put with MD5 checksum
  def Put(block_number, data):
    
    global SERVER_HITS
    SERVER_HITS = SERVER_HITS + 1
    RawBlocks.block[block_number] = data.data
    checksum = md5_checksum(RawBlocks.block[block_number])
    
    if block_number == CORRUPTED_BLOCK:
      checksum = md5_checksum(CORRUPT_DATA)
      # no point of this because we send the corrected data to the output even after
      # storing the corrupt data block message. This is because of recoverblock
      # we cant print the block corrupted show message.
      RawBlocks.block[block_number] = CORRUPT_DATA_2

    RawBlocks.checksums[block_number] = checksum
    RawBlocks.Sleep()
    return 0

  server.register_function(Put)

  def RSM(block_number):

    global SERVER_HITS
    SERVER_HITS = SERVER_HITS + 1
    RSM_LOCKED = bytearray(b'\x01') * 1
    result = RawBlocks.block[block_number]
    # RawBlocks.block[block_number] = RSM_LOCKED
    #since adding new value to the block need to update the checksum.
    inital_checksum = md5_checksum(result)
    if inital_checksum == RawBlocks.checksums[block_number]: 
      put_data = bytearray(RSM_LOCKED.ljust(BLOCK_SIZE,b'\x01'))
      RawBlocks.block[block_number] = put_data
      checksum = md5_checksum(put_data)
      RawBlocks.checksums[block_number] = checksum
      RawBlocks.Sleep()
    return result

  server.register_function(RSM)

  def ServerLoad():
    global SERVER_HITS
    SERVER_HITS = SERVER_HITS + 1
    return SERVER_HITS
  
  server.register_function(ServerLoad)

  # Run the server's main loop
  print ("Running block server with nb=" + str(TOTAL_NUM_BLOCKS) + ", bs=" + str(BLOCK_SIZE) + " on port " + str(PORT))
  server.serve_forever()