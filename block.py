import pickle, logging
import fsconfig
import xmlrpc.client, socket, time

#### BLOCK LAYER

# global TOTAL_NUM_BLOCKS, BLOCK_SIZE, INODE_SIZE, MAX_NUM_INODES, MAX_FILENAME, INODE_NUMBER_DIRENTRY_SIZE

class DiskBlocks():
    def __init__(self):

        # initialize clientID
        if fsconfig.CID >= 0 and fsconfig.CID < fsconfig.MAX_CLIENTS:
            self.clientID = fsconfig.CID
        else:
            print('Must specify valid cid')
            quit()

        self.block_servers = []

        # initialize XMLRPC client connection to raw block server
        if fsconfig.START_PORT:
            START_PORT = fsconfig.START_PORT
        else:
            print('Must specify port number')
            quit()

        if fsconfig.MAX_SERVERS:
            MAX_SERVERS = fsconfig.MAX_SERVERS
        else:
            print('specify total number of servers > 2')
            quit()
       
        for i in range(MAX_SERVERS):

            PORT_ID = START_PORT + i
            server_url = 'http://' + fsconfig.SERVER_ADDRESS + ':' + str(PORT_ID)
            block_server = xmlrpc.client.ServerProxy(server_url, use_builtin_types=True)
            self.block_servers.append(block_server)
            socket.setdefaulttimeout(fsconfig.SOCKET_TIMEOUT)
        
        # initialize block cache empty
        self.blockcache = {}

        self.bad_server = -1

        self.RSM_BLOCK_SERVER,_,_,_ = self.virtual_to_physical(fsconfig.TOTAL_NUM_BLOCKS-1)
        print(f'THE RSM SERVER IS {self.RSM_BLOCK_SERVER}')

    def xor_blocks(self,b1,b2):
    
        xor = bytearray()
        for a1,a2 in zip(b1,b2):
            xor.append(a1^a2)
        
        return xor
        
    # def virtual_to_physical(self,virtual_block_number):
    
    #     set_size = fsconfig.MAX_SERVERS - 1
    #     if(set_size >= 1):
            
    #         # determine the set number and position within that set for the virtual blocks
    #         set_number = virtual_block_number // set_size
    #         pos_in_set = virtual_block_number % set_size

    #         #calculate the physical location for data block
    #         data_server = (set_number + pos_in_set) % fsconfig.MAX_SERVERS
    #         data_block_number = set_number

    #         #calculate the physical location for the parity block
    #         parity_server = set_size - set_number % fsconfig.MAX_SERVERS
    #         parity_block_number = data_block_number

    #         if parity_server<= data_server:
    #             data_server+=1

    #         return data_server, data_block_number, parity_server, parity_block_number

    def virtual_to_physical(self,virtual_block_numb):
    
        # Generate Parity information
        NumServers = fsconfig.MAX_SERVERS
        parity_block_numb = virtual_block_numb // (NumServers-1)
        # effective roation of parity across different server in 
        # anticlockwise direction
        parity_id = (NumServers -1) -((parity_block_numb) % (NumServers))

        # Generate physical block locations
        block_server_index = virtual_block_numb % (NumServers-1)
        actual_block_num = (virtual_block_numb // (NumServers-1))

        #Check for indexing issues
        if parity_id <= block_server_index:
            block_server_index = block_server_index + 1


        return block_server_index,actual_block_num,parity_id,parity_block_numb
        
    def RecoverBlock(self,server_id,block_number,virtual_block_number):

        put_data = bytearray(fsconfig.BLOCK_SIZE)
        for i in range(fsconfig.MAX_SERVERS):
            if i!=server_id:
                server_data = self.SingleGet(i,block_number,virtual_block_number)
                put_data = bytearray(self.xor_blocks(put_data,server_data))
        return put_data
    
    def RepairServer(self,server_id):

        if server_id == self.bad_server:
            self.bad_server = -1
            num_server_blocks = fsconfig.TOTAL_NUM_BLOCKS // (fsconfig.MAX_SERVERS-1)
            for block in range(num_server_blocks):
                put_data_block = self.RecoverBlock(server_id,block,0) # here 0 cause don't need the virtual block number here.
                self.SinglePut(server_id,block,put_data_block)
            print(f'Server Repaird {server_id}')

    #get the total number of hits, individual hits and average server load
    def ShowLoad(self):

        total_hits = 0
        for i in range(fsconfig.MAX_SERVERS):
            if i!=self.bad_server:
                try:
                    server_hit = self.block_servers[i].ServerLoad()
                    print(f'Server [{i}] requests = {server_hit}')
                except (socket.timeout, ConnectionError, ConnectionRefusedError) as e:
                    server_hit = 0
                    print(f'Server [{i}] requests = {server_hit}')
                    print(f"SERVER_TIMED_OUT due to {e} for server {i}")
                    print(f'SERVER_DISCONNECTED ShowLoad')
                    time.sleep(fsconfig.RETRY_INTERVAL)
            else:
                server_hit = 0
                print(f"SERVER_TIMED CONNECTED_ERROR for server {i}")
                print(f'SERVER_DISCONNECTED ShowLoad')
                time.sleep(fsconfig.RETRY_INTERVAL)
            total_hits = total_hits + server_hit
        
        print(f'The total server requests = {total_hits}')
        average_hits = total_hits//fsconfig.MAX_SERVERS
        print(f'The average server hits = {average_hits}')

        return 0
        
    def SingleGet(self,server_id,block_number,virtual_block_number):

        if(server_id!=self.bad_server):
            try:
                data = self.block_servers[server_id].Get(block_number)
                #print(f'dat from single get {data}')
            except (socket.timeout, ConnectionRefusedError) as e:
                self.bad_server = server_id
                data = self.RecoverBlock(server_id,block_number,virtual_block_number)
                print(f"SERVER_TIMED_OUT due to {e} for server {server_id}")
                print(f'SERVER_DISCONNECTED GET {virtual_block_number}')
                time.sleep(fsconfig.RETRY_INTERVAL)
        else:
            print(f'SERVER_DISCONNECTED GET {virtual_block_number}')
            data = self.RecoverBlock(server_id,block_number,virtual_block_number)
            print('block recovered')
        return data
    
    def SinglePut(self,server_id,block_number,put_data_block):
        
        if server_id!=self.bad_server:
            try:
                ret = self.block_servers[server_id].Put(block_number,put_data_block)
            except (socket.timeout, ConnectionRefusedError) as e:
                print(f"SERVER_TIMED_OUT due to {e} for server {server_id}")
                self.bad_server = server_id
                print(f'SERVER_DISCONNECTED {server_id}')
                ret = -1
                time.sleep(fsconfig.RETRY_INTERVAL)
                
        else:
            ret = -1
        
        return ret

    ## Put: interface to write a raw block of data to the block indexed by block number
    ## Blocks are padded with zeroes up to BLOCK_SIZE

    def Put(self, block_number, block_data):

        logging.debug(
            'Put: block number ' + str(block_number) + ' len ' + str(len(block_data)) + '\n' + str(block_data.hex()))
        if len(block_data) > fsconfig.BLOCK_SIZE:
            logging.error('Put: Block larger than BLOCK_SIZE: ' + str(len(block_data)))
            quit()

        if block_number in range(0, fsconfig.TOTAL_NUM_BLOCKS):
            
            #first find the physical server and block for data and parity  
            data_id,data_block,parity_id,parity_block = self.virtual_to_physical(block_number)
            
            #data for block
            put_data_block = bytearray(block_data.ljust(fsconfig.BLOCK_SIZE,b'\x00'))
            # if(block_number == fsconfig.TOTAL_NUM_BLOCKS-2):
            #     print(put_data_block)
            
            #data for parity # caution need to add if the server is present or not
            old_data_block = self.SingleGet(data_id,data_block,block_number)
            #check if old_data_block is not corrupted
            if old_data_block == -1:
                print(f'CORRUPTED_BLOCK {block_number}')
                old_data_block = self.RecoverBlock(data_id,data_block,block_number)
            old_parity_block = self.SingleGet(parity_id,parity_block,block_number)
            #check if old_parity_block is not corrupted
            if old_parity_block == -1:
                print(f'CORRUPTED_BLOCK {block_number}')
                old_parity_block = self.RecoverBlock(parity_id,parity_block,block_number)

            #print(old_data_block)
            #print(old_parity_block)
            im_parity_block = self.xor_blocks(old_data_block,put_data_block)
            new_parity_block = self.xor_blocks(im_parity_block,old_parity_block)

            #store data # caution need to add if the server is present or not
            if self.SinglePut(data_id,data_block,put_data_block) == -1:
                print(f'server down {data_id}')
                print(f'SERVER_DISCONNECTED PUT {block_number}')
            if self.SinglePut(parity_id,parity_block,new_parity_block) == -1:
                print(f'server_down {parity_id}')
                print(f'SERVER_DISCONNECTED PUT {block_number}')

            # update block cache #need to add conditon for showing cache.
            if fsconfig.CACHE:
                print('CACHE_WRITE_THROUGH ' + str(block_number))
            self.blockcache[block_number] = put_data_block

            """
            Below code is for updating the last client id in the block , haven't implemented if server goes down. 
            """
            #flag this is the last writer
            #unless this is a release - which doesn't flag last writer
            
            if block_number < fsconfig.TOTAL_NUM_BLOCKS-2:
                LAST_WRITER_BLOCK = fsconfig.TOTAL_NUM_BLOCKS - 2
                updated_block = bytearray(self.clientID.to_bytes(fsconfig.BLOCK_SIZE,byteorder = 'big'))
                data_id,data_block,parity_id,parity_block = self.virtual_to_physical(LAST_WRITER_BLOCK)
                old_data_block = self.SingleGet(data_id,data_block,LAST_WRITER_BLOCK)
                #check if old_data_block is not corrupted
                if old_data_block == -1:
                    print(f'CORRUPTED_BLOCK {block_number}')
                    old_data_block = self.RecoverBlock(data_id,data_block,block_number)
                old_parity_block = self.SingleGet(parity_id,parity_block,LAST_WRITER_BLOCK)
                #check if old_parity_block is not corrupted
                if old_parity_block == -1:
                    print(f'CORRUPTED_BLOCK {block_number}')
                    old_parity_block = self.RecoverBlock(parity_id,parity_block,block_number)

                im_parity_block = self.xor_blocks(old_data_block,updated_block)
                new_parity_block = self.xor_blocks(im_parity_block,old_parity_block)

                if self.SinglePut(data_id,data_block,updated_block) == -1:
                    print(f'server down {data_id}')
                    print(f'SERVER_DISCONNECTED PUT {LAST_WRITER_BLOCK}')
                if self.SinglePut(parity_id,parity_block,new_parity_block) == -1:
                    print(f'server_down {parity_id}')
                    print(f'SERVER_DISCONNECTED PUT {LAST_WRITER_BLOCK}')
                
                # try:
                #     self.SinglePut(data_id,data_block,updated_block)
                # except socket.timeout:
                #     print("SERVER_TIMED_OUT")
                #     time.sleep(fsconfig.RETRY_INTERVAL)

            # if ret == -1:
            #     logging.error('Put: Server returns error')
            #     quit()
            return 0
        else:
            logging.error('Put: Block out of range: ' + str(block_number))
            quit()


    ## Get: interface to read a raw block of data from block indexed by block number
    ## Equivalent to the textbook's BLOCK_NUMBER_TO_BLOCK(b)

    def Get(self, block_number):

        logging.debug('Get: ' + str(block_number))
        if block_number in range(0, fsconfig.TOTAL_NUM_BLOCKS):
            # logging.debug ('\n' + str((self.block[block_number]).hex()))
            # commenting this out as the request now goes to the server
            # return self.block[block_number]
            # call Get() method on the server
            # don't look up cache for last two blocks
            if (block_number < fsconfig.TOTAL_NUM_BLOCKS-2) and (block_number in self.blockcache):
                if fsconfig.CACHE:
                    print('CACHE_HIT '+ str(block_number))
                data = self.blockcache[block_number]
            else:
                if fsconfig.CACHE:
                    print('CACHE_MISS ' + str(block_number))
                data_id,data_block,parity_id,parity_block = self.virtual_to_physical(block_number)
                data = self.SingleGet(data_id,data_block,block_number)
                #if(block_number == fsconfig.TOTAL_NUM_BLOCKS-2):
                    #print(data)
                    #print(f'data id {data_id} data_block {data_block} parity_id {parity_id} parity_block {parity_block}')

                #checking if data is corrupt or not
                if data == -1:
                    print(f'CORRUPTED_BLOCK {block_number}')
                    data = self.RecoverBlock(data_id,data_block,block_number)
                # add to cache
                self.blockcache[block_number] = data
            # return as bytearray
            return bytearray(data)

        logging.error('DiskBlocks::Get: Block number larger than TOTAL_NUM_BLOCKS: ' + str(block_number))
        quit()

## RSM: read and set memory equivalent

    def RSM(self, block_number):
        logging.debug('RSM: ' + str(block_number))
        if block_number in range(0, fsconfig.TOTAL_NUM_BLOCKS):
            data_id,data_block,parity_id,parity_block = self.virtual_to_physical(block_number)
            data = self.block_servers[data_id].RSM(data_block)
            return bytearray(data)

        logging.error('RSM: Block number larger than TOTAL_NUM_BLOCKS: ' + str(block_number))
        quit()

        ## Acquire and Release using a disk block lock

    def Acquire(self):
        logging.debug('Acquire')
        RSM_BLOCK = fsconfig.TOTAL_NUM_BLOCKS - 1
        rsm_id,_,_,_ = self.virtual_to_physical(RSM_BLOCK)
        #print(f'RSM BLOCK SERVER IS {rsm_id}')
        lockvalue = self.RSM(RSM_BLOCK)
        logging.debug("RSM_BLOCK Lock value: " + str(lockvalue))
        while lockvalue[0] == 1:  # test just first byte of block to check if RSM_LOCKED
            logging.debug("Acquire: spinning...")
            lockvalue = self.RSM(RSM_BLOCK)
        # once the lock is acquired, check if need to invalidate cache
        self.CheckAndInvalidateCache()
        return 0

    def Release(self):
        logging.debug('Release')
        RSM_BLOCK = fsconfig.TOTAL_NUM_BLOCKS - 1
        # Put()s a zero-filled block to release lock
        self.Put(RSM_BLOCK,bytearray(fsconfig.RSM_UNLOCKED.ljust(fsconfig.BLOCK_SIZE, b'\x00')))
        return 0

    def CheckAndInvalidateCache(self):
        LAST_WRITER_BLOCK = fsconfig.TOTAL_NUM_BLOCKS - 2
        last_writer = self.Get(LAST_WRITER_BLOCK)
        last_writer = int.from_bytes(last_writer,byteorder='big')
        #print("last writer is ",last_writer)
        #print("client id is ",self.clientID)
        # if ID of last writer is not self, invalidate and update
        if last_writer != self.clientID:
            if fsconfig.CACHE:
                print("CACHE_INVALIDATED")
            self.blockcache.clear()
            #print(self.clientID)
            updated_block = bytearray(self.clientID.to_bytes(fsconfig.BLOCK_SIZE,byteorder = 'big'))
            #print(updated_block)
            self.Put(LAST_WRITER_BLOCK,updated_block)


        


    ## Serializes and saves the DiskBlocks block[] data structure to a "dump" file on your disk

    def DumpToDisk(self, filename):

        logging.info("DiskBlocks::DumpToDisk: Dumping pickled blocks to file " + filename)
        file = open(filename,'wb')
        file_system_constants = "BS_" + str(fsconfig.BLOCK_SIZE) + "_NB_" + str(fsconfig.TOTAL_NUM_BLOCKS) + "_IS_" + str(fsconfig.INODE_SIZE) \
                            + "_MI_" + str(fsconfig.MAX_NUM_INODES) + "_MF_" + str(fsconfig.MAX_FILENAME) + "_IDS_" + str(fsconfig.INODE_NUMBER_DIRENTRY_SIZE)
        pickle.dump(file_system_constants, file)
        pickle.dump(self.block, file)

        file.close()

    ## Loads DiskBlocks block[] data structure from a "dump" file on your disk

    def LoadFromDump(self, filename):

        logging.info("DiskBlocks::LoadFromDump: Reading blocks from pickled file " + filename)
        file = open(filename,'rb')
        file_system_constants = "BS_" + str(fsconfig.BLOCK_SIZE) + "_NB_" + str(fsconfig.TOTAL_NUM_BLOCKS) + "_IS_" + str(fsconfig.INODE_SIZE) \
                            + "_MI_" + str(fsconfig.MAX_NUM_INODES) + "_MF_" + str(fsconfig.MAX_FILENAME) + "_IDS_" + str(fsconfig.INODE_NUMBER_DIRENTRY_SIZE)

        try:
            read_file_system_constants = pickle.load(file)
            if file_system_constants != read_file_system_constants:
                print('DiskBlocks::LoadFromDump Error: File System constants of File :' + read_file_system_constants + ' do not match with current file system constants :' + file_system_constants)
                return -1
            block = pickle.load(file)
            for i in range(0, fsconfig.TOTAL_NUM_BLOCKS):
                self.Put(i,block[i])
            return 0
        except TypeError:
            print("DiskBlocks::LoadFromDump: Error: File not in proper format, encountered type error ")
            return -1
        except EOFError:
            print("DiskBlocks::LoadFromDump: Error: File not in proper format, encountered EOFError error ")
            return -1
        finally:
            file.close()


## Prints to screen block contents, from min to max

    def PrintBlocks(self,tag,min,max):
        print ('#### Raw disk blocks: ' + tag)
        for i in range(min,max):
            print ('Block [' + str(i) + '] : ' + str((self.Get(i)).hex()))
