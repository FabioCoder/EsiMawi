import json
from datetime import datetime

DATE_FORMAT = '%Y-%m-%d'


class Receiving:
    def __init__(self, receiving_id, receiving_date):
        self.id = receiving_id
        self.receiving_date = receiving_date

    def toJson(self):
        return json.dumps({
            "id": self.id,
            "receiving_date": datetime.strftime(self.receiving_date, DATE_FORMAT),
        })

    def getReceivingId(self):
        return self.id

    def getReceivingDate(self):
        return self.receiving_date

    @classmethod
    def eventToObject(cls, event):
        receiving_date = datetime.strptime(
                                    event.get('receiving_date', datetime.now().strftime(DATE_FORMAT)), DATE_FORMAT)
        return Receiving(-1, receiving_date)

    @classmethod
    def rowToObject(cls, row):
        return Receiving(row['id'], row['receiving_date'])



