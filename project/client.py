# saved as greeting-client.py
import threading
import Pyro5.api

class Client(threading.Thread):
    files = {}
    lock = None
    tracker_uri = None
    name = ''

    def run(self):
        '''
        while(True):
            uri = input("Informe a URI do servidor:").strip()
            filename = input("Informe o nome do arquivo:").strip()

            peer = Pyro5.api.Proxy(uri)
            print("Arquivo retornado: " + peer.getFile(filename))
            print("\n\n\n\n")
        '''
        self.lock = threading.Lock()

        while(True):
            selected_option = input(
                "Selecione uma opção:\n"+
                "[1] Compartilhar arquivo\n"+
                "[2] Consultar arquivos\n"+
                "[3] Baixar arquivo\n\n" +
                "Escolhas: "
            )

            print("\n\n\n")

            match(selected_option):
                case "1":
                    file_path = input("Informe o local do arquivo: ")
                    file_name = input("Informe o nome de compartilhamento do arquivo: ")

                    self.add_file(file_name, file_path)

                case "2":
                    self.list_files()

                case "3":
                    uri = input("Informe a URI do peer: ")
                    file_name = input("Informe o nome do arquivos: ")
    
    def add_file(self, name, path):
        with open(path, "rb") as file:
            file_bytes = file.read()
        
        with self.lock:
            self.files[name] = file_bytes

        try:
            tracker = Pyro5.api.Proxy(self.tracker_uri)
            print(tracker.register_file(self.name, [name]))
        except:
            raise Exception("Erro ao abrir o arquivo!")
    

    def list_files(self):
        tracker = Pyro5.api.Proxy(self.tracker_uri)

        print("Arquivos listado: ")
        print(tracker.list_files())
        print("\n\n\n")


    def download_file(self, uri, file_name):
        file_bytes = Pyro5.api.Proxy(uri).get_file(file_name)
        
        with open(f"downloads/{file_name}", "wb") as file:
            file.write(file_bytes)

        print("Arquivo baixado com sucesso!")


    # get file chamado pelo peer