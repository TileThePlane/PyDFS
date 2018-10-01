import rpyc
import uuid
import os

from rpyc.utils.server import ThreadedServer

DATA_DIR="/tmp/minion/"

class MinionService(rpyc.Service):
  class exposed_Minion():
    blocks = {}

    def exposed_put(self,block_uuid,data,minions):
      try:
        with open(DATA_DIR+str(block_uuid),'w') as f:
            f.write(data)
      except IOError as e:
        print "I/O error({0}): {1}".format(e.errno, e.strerror)
      except:
        print "Unexpected error:", sys.exc_info()[0]
        if len(minions)>0:
          self.forward(block_uuid,data,minions)


    def exposed_get(self,block_uuid):
      block_addr=DATA_DIR+str(block_uuid)
      if not os.path.isfile(block_addr):
        return None
      try:
        with open(block_addr) as f:
          return f.read()
      except IOError as e:
        print "I/O error({0}): {1}".format(e.errno, e.strerror)
      except:  # handle other exceptions such as attribute errors
        print "Unexpected error:", sys.exc_info()[0]  
 
    def forward(self,block_uuid,data,minions):
      print "8888: forwaring to:"
      print block_uuid, minions
      minion=minions[0]
      minions=minions[1:]
      host,port=minion

      con=rpyc.connect(host,port=port)
      minion = con.root.Minion()
      minion.put(block_uuid,data,minions)

    def delete_block(self,uuid):
      pass

if __name__ == "__main__":
  try:
    if not os.path.isdir(DATA_DIR): os.mkdir(DATA_DIR)
  except OSError as e:
    print "OS error({0}): {1}".format(e.errno, e.strerror)
    
  t = ThreadedServer(MinionService, port = 8888)
  t.start()
