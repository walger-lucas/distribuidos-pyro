# Criar Peer que se conecte ao servidor de nomes ao iniciar, após isso escolha um 
# num aleatório para ser seu tempo de heartbeat.
import Pyro5
import random
import re
import time
import threading
import concurrent.futures
import Pyro5.api
from enum import Enum

class NodeState(Enum):
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3


class FileExchangePeer(object):
    state:NodeState = NodeState.FOLLOWER
    
    def __init__(self,name):
        self.name = name
        self.heartbeat_wait_s = random.randint(150,300)/1000.0 # choose rand int to start counting
        self.epoch_lock = threading.Lock()
        self.already_voted = False

        ns = Pyro5.api.locate_ns() 

        
        try:
            uri = ns.lookup("tracker")
            tracker_proxy = Pyro5.api.Proxy(uri)
            self.epoch = tracker_proxy.get_epoch()
            tracker_name = tracker_proxy.get_name()
        except:
            self.epoch = 0
            tracker_name = "EMPTY"
        
        print(f"Peer {self.name} started || Current Epoch {self.epoch} || TRACKER: {tracker_name}\n")

        
    
    def start(self):
        #start new threads for startup
        self._hb_lock = threading.Lock()
        self._hb_event = threading.Event()
        self._hb_thread = threading.Thread(target=self._hb_watchdog, daemon=True)
        self._hb_thread.start()
        
        #create thread for program (to add, remove and ask for files)
    
    #HEARTBEAT RECEIVER
    @Pyro5.api.expose
    def heartbeat(self):
        with self._hb_lock:
                self._hb_event.set()
                self._hb_event.clear()
    
    def _hb_watchdog(self):
        while True:
            triggered = self._hb_event.wait(self.heartbeat_wait_s)
            if not triggered:
                self.do_election()
                self.heartbeat()

    def _ask_for_vote(uri,epoch):
        try:
            return Pyro5.api.Proxy(uri).request_to_vote(epoch)
        except:
            return None
    
    def do_election(self):
        print(f"Starting Election\n")
        with self.epoch_lock:
            self.state = NodeState.CANDIDATE
            self.epoch = self.epoch+1
            self.already_voted = False
            election_epoch = self.epoch
            all_names = Pyro5.api.locate_ns().list()
            election_uris = []
            for names,uri in all_names.items():
                if(names[:4]=="peer"):
                    election_uris.append(uri)
            
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(election_uris)) as executor:
            futures = {executor.submit(FileExchangePeer._ask_for_vote, peer_uri,election_epoch): peer_uri for peer_uri in election_uris}
            done, not_done = concurrent.futures.wait(futures,timeout=0.050)
            votes = 0
            total = 0
            for vote in done :
                result = vote.result()
                if(result == None):
                    continue
                total = total+1
                votes = votes + (1 if result else 0)
            print(f"Got {votes} votes out of {total} on epoch {election_epoch}\n")
        with self.epoch_lock:
            if(self.state == NodeState.CANDIDATE and election_epoch == self.epoch and votes>total/2):
                self.state = NodeState.LEADER
                try:
                    ns = Pyro5.api.locate_ns() 
                    ns.remove("tracker")
                except: pass
                ns.register("tracker",all_names[f"peer.{self.name}"])
                print("Became new leader")

                self._hbsend_thread = threading.Thread(target=self.send_heartbeats, daemon=True)
                
                ns_items = ns.list()
                tracker_uri =ns_items[f"tracker"]
                for name,cur_uri in ns_items.items():
                    if(name[:4]=="peer"):
                        try:  
                            Pyro5.api.Proxy(cur_uri).update_leader(election_epoch,tracker_uri)
                        except: pass
                self._hbsend_thread.start()
                


    def send_heartbeats(self):
        ns = Pyro5.api.locate_ns()
        with self.epoch_lock:
            leader = self.state == NodeState.LEADER

        while(leader):

            all_items = ns.list()
            for name,uri in all_items.items():
                    try:
                        Pyro5.api.Proxy(uri).heartbeat()
                    except: pass
            time.sleep(0.1)
            with self.epoch_lock:
                leader = self.state == NodeState.LEADER
    
    @Pyro5.api.expose
    def get_epoch(self):
        with self.epoch_lock:
            return self.epoch
        
    @Pyro5.api.expose
    def get_name(self):
        return self.name



    @Pyro5.api.expose
    def request_to_vote(self,epoch) -> bool:
        self.heartbeat() # restart heartbeat as time to wait until end of election to start another election
        with self.epoch_lock:
            #if self epoch is less then the current election's it is in a new term and has a new vote
            if(self.epoch < epoch):
                self.epoch = epoch
                self.already_voted = False
                self.state = NodeState.FOLLOWER
            elif (self.epoch > epoch):  # if it is from an older term, it is not a valid election anymore, disregard
                print(f"Voted No on Epoch {epoch}\n")
                return False
            
            if(self.already_voted): #if already voted, cannot vote true again
                print(f"Voted No on Epoch {epoch}\n")
                return False
            #vote true if first vote and remove vote capability
            self.already_voted = True 
            print(f"Voted Yes on Epoch {epoch}\n")
            return True

    @Pyro5.api.expose  
    def update_leader(self,epoch,uri):
        if(self.epoch>epoch):
            return
        self.epoch = epoch
        tracker_proxy = Pyro5.api.Proxy(uri)
        print(f"Updated Leader to {tracker_proxy.get_name()} on epoch {epoch}")
        #send data to new tracker TODO


    




def main():
    
    daemon = Pyro5.server.Daemon()
    try:
        ns = Pyro5.api.locate_ns() 
    except:
        print("INICIALIZE O SERVIÇO DE NOMES!!!")
        exit()
    name = re.sub(r'[^a-zA-Z0-9_-]', '', input("Nomeie este Processo:").strip().lower().replace(" ","_"))
    name_len = len(name)

    if(name_len>20 or name_len < 5):
        print(f"Comprimento de {name} é {name_len}, o máximo é 20, mínimo é 5\nTerminando processo\n")
        exit()
    print(f"Nome escolhido foi {name}\n")

    try:
        peer = FileExchangePeer(name=name)
        uri = daemon.register(peer)
        ns.register(f"peer.{peer.name}", uri)
    except:
        print("Algo deu errado\nTerminando processo\n")
        exit()
    peer.start()
    daemon.requestLoop()
    

    
    

main()