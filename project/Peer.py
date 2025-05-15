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
from client import Client

class NodeState(Enum):
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3


class FileExchangePeer(object):
    state:NodeState = NodeState.FOLLOWER
    file_list = {}
    tracker_uri = ''

    def __init__(self,name):
        self.name = name
        self.heartbeat_wait_s = random.randint(15,30) # choose rand int to start counting
        self.epoch_lock = threading.Lock()
        self.already_voted = False

        self.files_lock = threading.Lock()

        self.client = Client()
        self.client.name = name

        ns = Pyro5.api.locate_ns() 

        
        try:
            # Find tracker, get tacker proxy, init epoch storage in tracker
            self.tracker_uri = ns.lookup("tracker")
            tracker_proxy = Pyro5.api.Proxy(self.tracker_uri)
            self.epoch = tracker_proxy.get_epoch()
            tracker_name = tracker_proxy.get_name()
            
            self.client.tracker_uri = self.tracker_uri
        except:
            self.epoch = 0
            tracker_name = "EMPTY"
        
        print(f"Peer {self.name} started || Current Epoch {self.epoch} || TRACKER: {tracker_name}\n")

        
    
    def start(self):
        #start new threads for startup
        self._hb_lock = threading.Lock() # mutex para as operações de hearbeat
        self._hb_event = threading.Event() # sinalize o evento da thread
        self._hb_thread = threading.Thread(target=self._hb_watchdog, daemon=True)
        self._hb_thread.start() # Roda self._hb_watchdog em uma nova thread
        
        #create thread for program (to add, remove and ask for files)
        self.client.start()
    
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

    # Pergunta o voto ao peer para a eleição
    def _ask_for_vote(uri,epoch):
        try:
            return Pyro5.api.Proxy(uri).request_to_vote(epoch)
        except:
            return None
    
    def do_election(self):
        print(f"Starting Election\n")
        with self.epoch_lock: # bloqueando os recursos
            self.state = NodeState.CANDIDATE
            self.epoch = self.epoch+1
            self.already_voted = False
    
            # Inicia uma nova época e envia a sua candidatura
            # para todos os peers no servidor de nomes
            election_epoch = self.epoch
            all_names = Pyro5.api.locate_ns().list()
            election_uris = []
            for names,uri in all_names.items():
                if(names[:4]=="peer"):
                    election_uris.append(uri)
        
        # Inicia uma thread para cada peer
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(election_uris)) as executor:
            # Prepara a execução da pergunta pelo voto para cada peer
            futures = {executor.submit(FileExchangePeer._ask_for_vote, peer_uri,election_epoch): peer_uri for peer_uri in election_uris}
            done, not_done = concurrent.futures.wait(futures,timeout=0.050)
            votes = 0
            total = 0

            # Para cada thread que retornou, conta os votos que recebeu
            for vote in done :
                result = vote.result()
                if(result == None):
                    continue
                total = total+1
                votes = votes + (1 if result else 0)
            print(f"Got {votes} votes out of {total} on epoch {election_epoch}\n")

        with self.epoch_lock: # Bloqueando os recursos
            # Se é um cadidato e recebeu mais da metade dos votos na mesma eleição
            if(self.state == NodeState.CANDIDATE and election_epoch == self.epoch and votes>total/2):
                
                # Promove seu próprio estado e se registra como tracker
                self.state = NodeState.LEADER
                try:
                    ns = Pyro5.api.locate_ns() 
                    ns.remove("tracker")
                except: pass

                ns.register("tracker",all_names[f"peer.{self.name}"])
                print("Became new leader")
                
                # Inicia o envio do hearbeat
                self._hbsend_thread = threading.Thread(target=self.send_heartbeats, daemon=True)
                
                ns_items = ns.list()
                tracker_uri =ns_items[f"tracker"]
                for name,cur_uri in ns_items.items():
                    if(name[:4]=="peer"):
                        try:  
                            Pyro5.api.Proxy(cur_uri).update_leader(election_epoch,tracker_uri)
                        except: pass
                        
                print("send_heartbeats")
                self._hbsend_thread.start()
                

    # Envia o hearbat para todos os peers na lista
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
        self.tracker_uri = uri
        self.client.tracker_uri = uri
        self.epoch = epoch
        tracker_proxy = Pyro5.api.Proxy(uri)
        print(f"Updated Leader to {tracker_proxy.get_name()} on epoch {epoch}")
        
        tracker_proxy.register_file(self.name, self.client.files.keys())
        file_list = None

    # Registra arquivo no tracker
    @Pyro5.api.expose
    def register_file(self, peer_name, file_list):
        if self.state == NodeState.LEADER:
            with self.files_lock:
                if self.file_list == None:
                    self.file_list = {}
                    self.file_list[self.name] = self.client.files.keys()

                if peer_name in self.file_list:
                    self.file_list[peer_name].extend(file_list)
                else:
                    self.file_list[peer_name] = file_list

            message = f"{file_list} de {peer_name} registrado com sucesso!"

            print(message)

            return message

    @Pyro5.api.expose
    def list_files(self):
        return self.file_list

    @Pyro5.api.expose
    def get_file(self, filename):
        return self.client.files[filename]


    




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
        # Registra o processo para receber chamadas remotas
        peer = FileExchangePeer(name=name)
        uri = daemon.register(peer)
        ns.register(f"peer.{peer.name}", uri)
    except:
        print("Algo deu errado\nTerminando processo\n")
        exit()

    # Inicia o fluxo do processo
    peer.start()

    # Inicia o loop de escuta das requisições
    daemon.requestLoop()
    

    
    

main()