from datalake.sdk import Entity
import datalake.etl.schema as sch

class MPAExecution(Entity):
    def __schema__() -> dict:
        return {
            "executionid": sch.Str(clean=True),
            "status": sch.Str(clean=True),
            "application": sch.Str(clean=True),
            "task": sch.Str(clean=True),
            "email": sch.Str(clean=True),
            "extra": sch.Str(clean=True),
            "created_at": sch.Time(utc=True),
            "updated_at": sch.Time(utc=True),
        }