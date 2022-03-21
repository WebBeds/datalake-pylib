from ..entity import Entity

from datalake.etl import (
    schema as sch
)

class Hotel(Entity):
    def __schema__():
        return {
            "accommodationtype": sch.Str(clean=True),
            "active": sch.Bool(),
            "address": sch.Str(clean=True),
            "chainid": sch.Str(clean=True),
            "chainname": sch.Str(clean=True),
            "cityid": sch.Str(clean=True),
            "cityname": sch.Str(clean=True),
            "countryid": sch.Str(clean=True),
            "countryname": sch.Str(clean=True),
            "creation": sch.Str(clean=True),
            "destinationid": sch.Str(clean=True),
            "destinationname": sch.Str(clean=True),
            "giataid": sch.Str(clean=True),
            "giatasource": sch.Str(clean=True),
            "hotelid": sch.Str(clean=True),
            "hotelname": sch.Str(clean=True),
            "lastmodified": sch.Str(clean=True),
            "latitude": sch.Str(clean=True),
            "longitude": sch.Str(clean=True),
            "platform": sch.Str(clean=True),
            "platformcountryid": sch.Str(clean=True),
            "postcode": sch.Str(clean=True),
            "resortid": sch.Str(clean=True),
            "resortname": sch.Str(clean=True),
            "starrating": sch.Str(clean=True),
            "starratingid": sch.Str(clean=True),
            "stateid": sch.Str(clean=True),
            "statename": sch.Str(clean=True),
            "streetname": sch.Str(clean=True),
            "telephone": sch.Str(clean=True),
            "tticode": sch.Str(clean=True),
            "updated_at": sch.Str(clean=True),
            "wbpropertyid": sch.Str(clean=True),
            "webbedsregion": sch.Str(clean=True),
            "webbedssubregion": sch.Str(clean=True),
            "webbedssubsubregion": sch.Str(clean=True)
        }