# src/config/settings.py

import os


class ClsSettings:
    # # Configurações do MongoDB
    # MONGO_HOST = os.getenv('MONGO_HOST', 'localhost')
    # MONGO_PORT = int(os.getenv('MONGO_PORT', 27017))
    # MONGO_DB_NAME = os.getenv('MONGO_DB_NAME', 'db_craam')
    # MONGO_USER = os.getenv('MONGO_USER', '')
    # MONGO_PASSWORD = os.getenv('MONGO_PASSWORD', '')
    # MONGO_BATCH_SIZE_TO_INSERT = 100000
    # @staticmethod
    # def get_mongo_uri():
    #     if ClsSettings.MONGO_USER and ClsSettings.MONGO_PASSWORD:
    #         return f"mongodb://{ClsSettings.MONGO_USER}:{ClsSettings.MONGO_PASSWORD}@{ClsSettings.MONGO_HOST}:{ClsSettings.MONGO_PORT}/{ClsSettings.MONGO_DB_NAME}"
    #     return f"mongodb://{ClsSettings.MONGO_HOST}:{ClsSettings.MONGO_PORT}/{ClsSettings.MONGO_DB_NAME}"

    # Mongo Collections
    MONGO_COLLECTION_FILE_QUEUE = 'file_queue'
    MONGO_COLLECTION_PROCESSED_FILE_TRACE = 'processed_file_trace'
    MONGO_COLLECTION_DATA_SST_RF_FILE_05MS: str = 'data_SST_rf_file_05ms'
    MONGO_COLLECTION_DATA_SST_RS_FILE_40MS = 'data_SST_rs_file_40ms'
    MONGO_COLLECTION_DATA_SST_BI_FILE_1S = 'data_SST_bi_file_1s'
    MONGO_COLLECTION_DATA_POEMAS_FILE_10ms = 'data_POEMAS_file_10ms'

    # Configurações do MongoDB AZURE (Cosmos DB via URI completa)
    MONGO_HOST = "mongosrv.mongocluster.cosmos.azure.com"
    MONGO_PORT = 27017  # Mantido por compatibilidade, não é usado com +srv
    MONGO_DB_NAME = "craam_data"
    MONGO_USER = "usrmongosrv"
    MONGO_PASSWORD = "Teste.100"
    MONGO_BATCH_SIZE_TO_INSERT = 100000

    @staticmethod
    def get_mongo_uri():
        return (
            f"mongodb+srv://{ClsSettings.MONGO_USER}:{ClsSettings.MONGO_PASSWORD}"
            f"@{ClsSettings.MONGO_HOST}/{ClsSettings.MONGO_DB_NAME}"
            "?tls=true&authMechanism=SCRAM-SHA-256&retrywrites=false&maxIdleTimeMS=120000"
        )

    @staticmethod
    def get_mongo_collection_name_by_file_type(file_path: str) -> str:
        # Extract the prefix from the file_path
        file_name = os.path.basename(file_path)
        prefix = file_name[:2]  # Assuming the prefix is the first two characters of the file name
        extension = os.path.splitext(file_name)[1].lower()  # Get the file extension

        if extension == ".trk":
            return ClsSettings.MONGO_COLLECTION_DATA_POEMAS_FILE_10ms
        elif prefix == "rf":
            return ClsSettings.MONGO_COLLECTION_DATA_SST_RF_FILE_05MS
        elif prefix == "rs":
            return ClsSettings.MONGO_COLLECTION_DATA_SST_RS_FILE_40MS
        elif prefix == "bi":
            return ClsSettings.MONGO_COLLECTION_DATA_SST_BI_FILE_1S
        else:
            raise ValueError("Unsupported prefix or extension in file_path: {}".format(file_path))


# Inicializa as configurações
settings = ClsSettings()
