# src/config/settings.py

import os


class ClsSettings:
     # # Configurações do MongoDB ** LOCAL **
     MONGO_HOST = os.getenv('MONGO_HOST', 'localhost')
     MONGO_PORT = int(os.getenv('MONGO_PORT', 27027)) #27031 STAND -27027 SHARDING
     MONGO_DB_DATA = os.getenv('MONGO_DB_DATA', 'craam_data')
     MONGO_DB_PORTAL = os.getenv('MONGO_DB_PORTAL', 'macksundb')
     MONGO_USER = os.getenv('MONGO_USER', '')
     MONGO_PASSWORD = os.getenv('MONGO_PASSWORD', '')
     MONGO_BATCH_SIZE_TO_INSERT = 100000

     # ===== MongoDB Azure (consumo via portal) =====
     MONGO_AZURE_HOST = os.getenv('MONGO_AZURE_HOST', 'mongosrv.mongocluster.cosmos.azure.com')
     MONGO_AZURE_DB = os.getenv('MONGO_AZURE_DB', 'macksundb')
     MONGO_AZURE_USER = os.getenv('MONGO_AZURE_USER', 'usrmongosrv')
     MONGO_AZURE_PASSWORD = os.getenv('MONGO_AZURE_PASSWORD', 'Teste.100')

    # ZENODO
     ZENODO_TOKEN = "vN0jaQTdNwmfhs4lg6abmdUwssTLnl6eSZpuKSVkYMSfyyrcfEJGmzEyqCuJ"
     ZENODO_BASE_URL = "https://zenodo.org/api"
     ZENODO_COMMUNITY = "conde_set"
     ZENODO_LICENSE = "cc-by-4.0"
     
     AZURE_BLOB_CONNECTION_STRING = os.getenv(
         'AZURE_BLOB_CONNECTION_STRING',
         'DefaultEndpointsProtocol=https;AccountName=arm2macksun;AccountKey=LIMQNROm7U2ZXiWRs/cCWdeRwhXf0BO4XkPO6eWwBk02SS8CXqPrD04zsOes3vFtrQqjAof3W5gH+AStxMBJOg==;EndpointSuffix=core.windows.net'
     )

     AZURE_BLOB_BASE_URL = os.getenv(
         'AZURE_BLOB_BASE_URL',
         'https://arm2macksun.blob.core.windows.net'
     )


     @staticmethod
     def get_mongo_data_uri():
         if ClsSettings.MONGO_USER and ClsSettings.MONGO_PASSWORD:
             return f"mongodb://{ClsSettings.MONGO_USER}:{ClsSettings.MONGO_PASSWORD}@{ClsSettings.MONGO_HOST}:{ClsSettings.MONGO_PORT}/{ClsSettings.MONGO_DB_DATA}"
         return f"mongodb://{ClsSettings.MONGO_HOST}:{ClsSettings.MONGO_PORT}/{ClsSettings.MONGO_DB_DATA}"

     @staticmethod
     def get_mongo_portal_uri():
         if ClsSettings.MONGO_USER and ClsSettings.MONGO_PASSWORD:
             return f"mongodb://{ClsSettings.MONGO_USER}:{ClsSettings.MONGO_PASSWORD}@{ClsSettings.MONGO_HOST}:{ClsSettings.MONGO_PORT}/{ClsSettings.MONGO_DB_PORTAL}"
         return f"mongodb://{ClsSettings.MONGO_HOST}:{ClsSettings.MONGO_PORT}/{ClsSettings.MONGO_DB_PORTAL}"



     @staticmethod
     def get_mongo_azure_uri():
         return (
             f"mongodb+srv://{ClsSettings.MONGO_AZURE_USER}:{ClsSettings.MONGO_AZURE_PASSWORD}"
             f"@{ClsSettings.MONGO_AZURE_HOST}/{ClsSettings.MONGO_AZURE_DB}"
             "?tls=true&authMechanism=SCRAM-SHA-256&retrywrites=false&maxIdleTimeMS=120000"
         )

    # # Mongo Collections
     MONGO_COLLECTION_FILE_INGESTION_QUEUE = 'queue_file_ingestion'
     MONGO_COLLECTION_PROCESSED_FILE_TRACE = 'processed_file_trace'
     MONGO_COLLECTION_PARTITION_MAP = 'partition_map'
     MONGO_COLLECTION_DATA_AVAILABILITY_STATS='global_data_statistics'
     MONGO_COLLECTION_GENERATE_FILE_QUEUE = "queue_generate_file_to_export_to_cloud"
     MONGO_COLLECTION_FILE_EXPORT_REGISTRY_TO_CLOUD='exported_files_to_cloud'
     #
    # # Configurações do MongoDB AZURE (Cosmos DB via URI completa)
    # MONGO_HOST = "mongosrv.mongocluster.cosmos.azure.com"
    # MONGO_PORT = 27017  # Mantido por compatibilidade, não é usado com +srv
    # MONGO_DB_NAME = "craam_data"
    # MONGO_USER = "usrmongosrv"
    # MONGO_PASSWORD = "Teste.100"
    # MONGO_BATCH_SIZE_TO_INSERT = 100000
    #
    # @staticmethod
    # def get_mongo_uri():
    #     return (
    #         f"mongodb+srv://{ClsSettings.MONGO_USER}:{ClsSettings.MONGO_PASSWORD}"
    #         f"@{ClsSettings.MONGO_HOST}/{ClsSettings.MONGO_DB_NAME}"
    #         "?tls=true&authMechanism=SCRAM-SHA-256&retrywrites=false&maxIdleTimeMS=120000"
    #     )
    #


# Inicializa as configurações
settings = ClsSettings()
