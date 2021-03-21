from PySide2.QtCore import QObject, Signal, Slot

class DbEventDispatcherSingleton(QObject):

    _initialized = False
    _instance = None

    def __init__(self):
        if not DbEventDispatcherSingleton._initialized:
            QObject.__init__(self)
            DbEventDispatcherSingleton._initialized = True

    def __new__(cls):
        ''' Singleton '''
        if DbEventDispatcherSingleton._instance is None:
            DbEventDispatcherSingleton._instance = super().__new__(cls)
        return DbEventDispatcherSingleton._instance


    wellsAdded = Signal()
    wellsRemoved = Signal()
