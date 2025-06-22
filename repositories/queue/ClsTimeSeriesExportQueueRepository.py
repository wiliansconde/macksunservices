from repositories.base_repositories.ClsMongoHelper import ClsMongoHelper
from bson.objectid import ObjectId
from datetime import datetime

class ClsTimeSeriesExportQueueRepository:

    @staticmethod
    def get_next_pending_request():
        collection = ClsMongoHelper.get_portal_collection("time_series_search_history")
        request = collection.find_one_and_update(
            {"status": "PENDING"},
            {"$set": {"status": "IN_PROCESS", "lastUpdated": datetime.utcnow()}},
            sort=[("createdAt", 1)],
            return_document=True
        )
        return request

    @staticmethod
    def update_status(request_id, status):
        collection = ClsMongoHelper.get_portal_collection("time_series_search_history")
        collection.update_one(
            {"_id": ObjectId(request_id)},
            {"$set": {"status": status, "lastUpdated": datetime.utcnow()}}
        )
