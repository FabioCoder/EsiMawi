import json

class StockEntry:
    def __init__(self, fkplaces, fkmaterials, quantity, opened):
        self.fkplaces = fkplaces
        self.fkmaterials = fkmaterials
        self.quantity = quantity
        self.opened = opened

    def getFkPlaces(self):
        return self.fkplaces

    def getFkMaterials(self):
        return self.fkmaterials

    def getQuantity(self):
        return self.quantity

    def getOpened(self):
        return self.opened

    def toJson(self):
        return json.dumps(self.__dict__)

    @classmethod
    def eventToObject(cls, event):
        return StockEntry(event.get('fkplaces'), event.get('fkmaterials', -1), event.get('quantity'),
                          event.get('opened', 0))