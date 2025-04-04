# src/config/settings.py

import os


class Settings:
    # Configurações do MongoDB
    # MONGO_HOST = os.getenv('MONGO_HOST', 'localhost')
    # MONGO_PORT = int(os.getenv('MONGO_PORT', 27017))
    # MONGO_DB_NAME = os.getenv('MONGO_DB_NAME', 'db_craam')
    # MONGO_USER = os.getenv('MONGO_USER', '')
    # MONGO_PASSWORD = os.getenv('MONGO_PASSWORD', '')
    #
    # @staticmethod
    # def get_mongo_uri():
    #     if Settings.MONGO_USER and Settings.MONGO_PASSWORD:
    #         return f"mongodb://{Settings.MONGO_USER}:{Settings.MONGO_PASSWORD}@{Settings.MONGO_HOST}:{Settings.MONGO_PORT}/{Settings.MONGO_DB_NAME}"
    #     return f"mongodb://{Settings.MONGO_HOST}:{Settings.MONGO_PORT}/{Settings.MONGO_DB_NAME}"

    MONGO_HOST = "mongosrv.mongocluster.cosmos.azure.com"
    MONGO_PORT = 27017  # Não usado para +srv, mas mantido por compatibilidade
    MONGO_DB_NAME = "macksundb"
    MONGO_USER = "usrmongosrv"
    MONGO_PASSWORD = "Teste.100"

    @staticmethod
    def get_mongo_uri():
        # Para Cosmos DB/MongoDB Atlas com +srv e parâmetros extras
        return (
            f"mongodb+srv://{Settings.MONGO_USER}:{Settings.MONGO_PASSWORD}"
            f"@{Settings.MONGO_HOST}/{Settings.MONGO_DB_NAME}"
            "?tls=true&authMechanism=SCRAM-SHA-256&retrywrites=false&maxIdleTimeMS=120000"
        )
# Inicializa as configurações
settings = Settings()
