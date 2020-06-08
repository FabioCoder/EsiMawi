import json

class Inventory:
    def __init__(self, fkplaces, place_description, fkmaterials, material_name,
                 material_size, quantity, material_measure, opened, minStock):
        self.fkplaces = fkplaces
        self.fkmaterials = fkmaterials
        self.material_name = material_name
        self.opened = opened
        self.quantity = quantity
        self.material_measure = material_measure
        self.material_size = material_size
        self.minStock = minStock
        self.place_description = place_description
        self.quantity_in_measure = material_size * quantity

    def toJson(self):
        return json.dumps(self.__dict__)

    @classmethod
    def rowToObject(cls, row):
        return Inventory(row['fkplaces'], row['description'], row['fkmaterials'], row['materialName'],
                         row['size'], row['quantity'], row['measure'], row['opened'], row['minStock'])
