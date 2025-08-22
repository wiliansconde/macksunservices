from datetime import datetime, timedelta

from models.partitioning.ClsPartition_map_model import ClsPartitionMapModel
from repositories.partitioning.ClsPartition_map_repository import ClsPartitionMapRepository
from enums.ClsResolutionEnum import ClsResolutionEnum
from enums.ClsInstrumentEnum import ClsInstrumentEnum

class ClsDataPartitionResolverService:

    def __init__(self):
        self.repository = ClsPartitionMapRepository()

    def get_target_collection(self, instrument: ClsInstrumentEnum, resolution: ClsResolutionEnum,
                              timestamp: datetime) -> str:
        try:
            start_date, end_date = self._get_date_range(timestamp, resolution)
            partitions = self.repository.find_partitions(instrument, resolution,  start_date, end_date)

            if partitions and len(partitions) > 0:
                collection_name = partitions[0].collection_name
                print(f"[Partitioning] Collection encontrada: {collection_name}")
                return collection_name

            print("[Partitioning] Nenhuma collection existente encontrada. Iniciando processo de criação...")

            # Define nome da nova collection e seu range
            collection_name = self._generate_collection_name(instrument, resolution, timestamp)
            #start_date, end_date = self._get_date_range(timestamp, resolution)

            # Verifica sobreposição
            if self.repository.check_overlap(instrument, resolution, start_date, end_date):
                error_msg = f"[Partitioning] Overlapping partition detected for {instrument.value} {resolution.value} {start_date} - {end_date}."
                print(error_msg)
                raise Exception(error_msg)

            # Cria nova entrada no mapa de partições
            new_partition = ClsPartitionMapModel(
                instrument=instrument.value,
                resolution=resolution.value,
                collection_name=collection_name,
                start_date=start_date,
                end_date=end_date,
                storage_backend="MongoDB",
                status="active",
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )

            try:
                self.repository.insert_partition(new_partition)
                self.repository.create_time_series_collection_if_not_exists(collection_name, resolution)
                print(f"[Partitioning] Nova collection criada com sucesso: {collection_name}")
            except Exception as insert_error:
                print(f"[Partitioning] Erro ao criar nova partition ou collection: {insert_error}")
                raise insert_error

            return collection_name

        except Exception as e:
            print(f"[Partitioning] Erro ao resolver target collection: {e}")
            raise e

    def get_collections_for_date_range(self, instrument: ClsInstrumentEnum, resolution: ClsResolutionEnum, start_date: datetime, end_date: datetime) -> list:
        partitions = self.repository.find_partitions(instrument, resolution, start_date, end_date)
        return [p.collection_name for p in partitions]



    def _generate_collection_name(self, instrument: ClsInstrumentEnum, resolution: ClsResolutionEnum,
                                  timestamp: datetime) -> str:
        year = timestamp.year
        partition_type = ClsResolutionEnum.get_partition_type(resolution.value)

        if partition_type == "monthly":
            month = str(timestamp.month).zfill(2)
            return f"data_{instrument.value}_{resolution.value}_{year}_{month}"

        elif partition_type == "semiannual":
            semester = "S1" if timestamp.month <= 6 else "S2"
            return f"data_{instrument.value}_{resolution.value}_{year}_{semester}"

        elif partition_type == "annual":
            return f"data_{instrument.value}_{resolution.value}_{year}"

        else:
            raise ValueError(f"Tipo de particionamento desconhecido para resolução: {resolution.value}")



    def _get_date_range(self, timestamp: datetime, resolution: ClsResolutionEnum):
        #PArtition_map definicao da granularidade. wconde
        resolution_value = resolution.value.lower()

        if resolution_value.endswith("ms"):
            numeric_value = int(resolution_value.replace("ms", ""))
            if numeric_value < 100:
                #  mensal
                start_date = datetime(timestamp.year, timestamp.month, 1)
                if timestamp.month == 12:
                    end_date = datetime(timestamp.year + 1, 1, 1) - timedelta(seconds=1)
                else:
                    end_date = datetime(timestamp.year, timestamp.month + 1, 1) - timedelta(seconds=1)
                return start_date, end_date
            else:
                #  semestral
                semester = 1 if timestamp.month <= 6 else 2
                start_month = 1 if semester == 1 else 7
                start_date = datetime(timestamp.year, start_month, 1)
                end_month = 6 if semester == 1 else 12
                end_day = 30 if end_month == 6 else 31
                end_date = datetime(timestamp.year, end_month, end_day, 23, 59, 59)
                return start_date, end_date
        else:
            #  1s ou acima = anual
            start_date = datetime(timestamp.year, 1, 1)
            end_date = datetime(timestamp.year, 12, 31, 23, 59, 59)
            return start_date, end_date


