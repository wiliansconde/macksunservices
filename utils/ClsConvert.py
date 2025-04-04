from datetime import datetime, timedelta


class ClsConvert:
    @staticmethod
    def get_full_datetime(path: str, number: int) -> datetime:
        """
        Obtém a data completa combinando a data extraída do caminho do arquivo com o tempo convertido do número.

        :param path: Caminho do arquivo.
        :param number: Número a ser convertido em horas, minutos, segundos e milissegundos.
        :return: Objeto datetime representando a data e hora completa.
        """

        def extract_date_from_path(path: str) -> datetime:
            """
            Extrai a data do caminho do arquivo.
            O caminho esperado tem o formato: ...\\YYYY\\M##\\D##\\...
            """
            try:
                # Dividir o caminho em partes
                parts = path.split('\\')
                # Inicializar variáveis
                year, month, day = None, None, None
                # Encontrar o ano, mês e dia no caminho
                for part in parts:
                    if part.isdigit() and len(part) == 4:  # Ano
                        year = int(part)
                    elif part.startswith('M') and part[1:].isdigit():  # Mês
                        month = int(part[1:])
                    elif part.startswith('D') and part[1:].isdigit():  # Dia
                        day = int(part[1:])

                # Verificar se todos os componentes foram encontrados
                if year is None or month is None or day is None:
                    raise ValueError("Formato de caminho de arquivo inválido para determinar a data do arquivo")

                date_str = f'{year}-{month:02d}-{day:02d}'
                return datetime.strptime(date_str, '%Y-%m-%d')
            except Exception as e:
                raise ValueError("Formato de caminho de arquivo inválido para determinar a data do arquivo") from e

        def convert_number_to_time(number: int) -> dict:
            """
            Converte o número para horas, minutos, segundos e milissegundos.
            """
            """hours = number / 36000000
            remainder = float(hours - int(hours))

            minutes = remainder * 60
            remainder = float(minutes - int(minutes))

            seconds = remainder * 60

            remainder = float(minutes - int(minutes))

            milliseconds = remainder % 1000"""

            hours = number / 36000000
            hours_int = int(hours)
            hours_decimal = hours - hours_int

            minutes = hours_decimal * 60
            minutes_int = int(minutes)
            minutes_decimal = minutes - minutes_int

            seconds = minutes_decimal * 60
            seconds_int = int(seconds)
            seconds_decimal = seconds - seconds_int

            milliseconds = seconds_decimal * 1000
            milliseconds_int = int(milliseconds)
            """
            if milliseconds ==0:
                aaa="ddddd"
            print({
                'hours': hours_int,
                'minutes': minutes_int,
                'seconds': seconds_int,
                'milliseconds': milliseconds_int
            })"""
            return {
                'hours': hours_int,
                'minutes': minutes_int,
                'seconds': seconds_int,
                'milliseconds': milliseconds_int
            }

        # Extrai a data do caminho do arquivo
        date = extract_date_from_path(path)
        # Converte o número para tempo
        time_dict = convert_number_to_time(number)
        # Combina a data e o tempo
        time_delta = timedelta(
            hours=time_dict['hours'],
            minutes=time_dict['minutes'],
            seconds=time_dict['seconds'],
            milliseconds=time_dict['milliseconds']
        )
        return date + time_delta

    def convert_bytes_to_mb(file_size_bytes: int) -> float:
        """
        Converte o tamanho do arquivo de bytes para megabytes.

        :param file_size_bytes: Tamanho do arquivo em bytes
        :return: String formatada com o tamanho do arquivo em megabytes
        """
        return round(file_size_bytes / (1024 * 1024), 3)

        #return f"Tamanho do arquivo: {size_in_mb:.2f} MB"