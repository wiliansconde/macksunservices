from datetime import datetime, timedelta

from models.partitioning.ClsPartition_map_model import ClsPartitionMapModel
from repositories.partitioning.ClsPartition_map_repository import ClsPartitionMapRepository
from enums.ClsResolutionEnum import ClsResolutionEnum
from enums.ClsInstrumentEnum import ClsInstrumentEnum

class ClsDataPartitionResolverService:

    def __init__(self):
        self.repository = ClsPartitionMapRepository()

    def get_target_collection(self, instrument: ClsInstrumentEnum, resolution: ClsResolutionEnum, timestamp: datetime) -> str:
        partitions = self.repository.find_partitions(instrument, resolution, timestamp, timestamp)
        if partitions:
            return partitions[0].collection_name
        else:
            collection_name = self._generate_collection_name(instrument, resolution, timestamp)
            start_date, end_date = self._get_date_range(timestamp, resolution)

            # Verifica sobreposição
            if self.repository.check_overlap(instrument, resolution, start_date, end_date):
                raise Exception(f"Overlapping partition detected for {instrument} {resolution} {start_date} - {end_date}")

            # Cria novo registro no mapa
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
            self.repository.insert_partition(new_partition)

            # Garante a criação física da collection como Time Series
            self.repository.create_time_series_collection_if_not_exists(collection_name, resolution)

            return collection_name

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


