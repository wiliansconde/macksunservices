import traceback


class GenericException(Exception):
    def __init__(self, original_exception, file_path):
        super().__init__(str(original_exception))
        self.file_path = file_path
        self.error_message = str(original_exception)
        self.error_trace = traceback.format_exc()
        self.handle_error()

    def handle_error(self):
        self.print_error()

    def print_error(self):
        # Imprime a mensagem de erro e o stack trace
        print(f"Erro no arquivo {self.file_path}: {self.error_message}\n{self.error_trace}")


