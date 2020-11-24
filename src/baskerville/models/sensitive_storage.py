import os

class SensitiveStorage(object):

    def __init__(self, path):
        super().__init__()
        self.path = path
        self.index_file_path = os.path.join(self.path, 'index.txt')


    def write(self, df):
        pass

    def read(self, df):
        pass



