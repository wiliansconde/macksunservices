from enum import Enum

class ClsResolutionEnum(Enum):
    Milliseconds_05 = "05ms"
    Milliseconds_10 = "10ms"
    Milliseconds_40 = "40ms"
    Milliseconds_100 = "100ms"
    Milliseconds_500 = "500ms"
    Seconds_01 = "1s"
    Seconds_05 = "5s"
    Minutes_01 = "1m"
    Hours_01 = "1h"

    @staticmethod
    def get_partition_type(resolution: str) -> str:
        """
        Define o tipo de particionamento (monthly, semiannual, annual) baseado na resolução.
        """
        try:
            numeric_part = ''.join([c for c in resolution if c.isdigit()])
            unit = ''.join([c for c in resolution if c.isalpha()]).lower()

            if unit == "ms":
                value = int(numeric_part)
                if value < 100:
                    return "monthly"
                else:
                    return "semiannual"
            else:
                # Qualquer resolução a partir de 1 segundo (1s, 5s, 1m, 1h) → Anual
                return "annual"

        except Exception as e:
            raise ValueError(f"Resolução inválida: {resolution}. Erro: {str(e)}")
