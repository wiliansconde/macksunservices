# src/repositories/config/ClsSystemConfigRepository.py

from typing import Any, Dict

from repositories.base_repositories.ClsMongoHelper import ClsMongoHelper


class ClsSystemConfigRepository:
    COLLECTION_NAME = "system_config"

    @staticmethod
    def get_partitioning_config() -> Dict[str, Any]:
        """
        Lê a configuração de particionamento em craam_master.system_config, _id = "partitioning".

        Esperado:
        {
          "_id": "partitioning",
          "status": "active",
          "partitioning": {
            "target_docs_per_collection": NumberLong(...),
            "sun_hours_per_day": NumberInt(...)
          }
        }
        """
        col = ClsMongoHelper.get_collection(ClsSystemConfigRepository.COLLECTION_NAME)

        doc = col.find_one({"_id": "partitioning", "status": "active"})
        if not doc:
            raise ValueError('system_config nao possui _id="partitioning" com status="active"')

        partitioning = doc.get("partitioning") or {}
        if "target_docs_per_collection" not in partitioning:
            raise ValueError('system_config.partitioning sem "target_docs_per_collection"')

        if "sun_hours_per_day" not in partitioning:
            raise ValueError('system_config.partitioning sem "sun_hours_per_day"')

        return partitioning
