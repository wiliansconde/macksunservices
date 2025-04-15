from datetime import datetime

class CLSConsolePrint:
    @staticmethod
    def _get_timestamp():
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    @staticmethod
    def info(message):
        print(f"{CLSConsolePrint._get_timestamp()} - INFO - {message}")

    @staticmethod
    def error(message):
        print(f"{CLSConsolePrint._get_timestamp()} - ERROR - {message}")

    @staticmethod
    def debug(message):
        print("")
        print(f"{CLSConsolePrint._get_timestamp()} - DEBUG - Mensagem: {message}\n")
