from dataclasses import dataclass, field
from config.config import DatabaseConfig
from sqlalchemy import text

from config.logging_config import configure_logging
logger = configure_logging(__name__)


@dataclass
class TypeInfo:
    type_id: int
    type_name: str = field(init=False)
    group_name: str = field(init=False)
    category_name: str = field(init=False)
    category_id: int = field(init=False)
    group_id: int = field(init=False)
    volume: int = field(init=False)
    def __post_init__(self):
        self.get_type_info()

    def get_type_info(self):
        db = DatabaseConfig("sde")
        stmt = text("SELECT * FROM inv_info WHERE typeID = :type_id")
        engine = db.engine
        with engine.connect() as conn:
            result = conn.execute(stmt, {"type_id": self.type_id})
            for row in result:
                self.type_name = row.typeName
                self.group_name = row.groupName
                self.category_name = row.categoryName
                self.category_id = row.categoryID
                self.group_id = row.groupID
                self.volume = row.volume
        engine.dispose()

if __name__ == "__main__":
    pass