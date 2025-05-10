import Pyro5.api
from main import files


class Peer(object):
    @Pyro5.api.expose
    def getFile(self, file_name):
        return files[file_name]

class Tracker:
    pass