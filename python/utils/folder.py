import os


class Folder:
    def __init__(self):
        pass

    @staticmethod
    def make_folder(path):
        if not os.path.exists(path):
            os.makedirs(path)
