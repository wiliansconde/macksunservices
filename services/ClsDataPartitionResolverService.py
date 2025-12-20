from datetime import datetime, timedelta
from typing import Optional, Tuple

from enums.ClsInstrumentEnum import ClsInstrumentEnum
from enums.ClsResolutionEnum import ClsResolutionEnum
from models.partitioning.ClsPartition_map_model import ClsPartitionMapModel
from repositories.partitioning.ClsPartition_map_repository import ClsPartitionMapRepository
from repositories.config.ClsSystemConfigRepository import ClsSystemConfigRepository


class ClsDataPartitionResolverService:

    EPOCH_ANCHOR = datetime(1970, 1, 1, 0, 0, 0)

    def __init__(self):
        self.repository = ClsPartitionMapRepository()
        self._partitioning_cfg_cache = None

    def get_target_collection(
        self,
        instrument: ClsInstrumentEnum,
        resolution: ClsResolutionEnum,
        timestamp: datetime
    ) -> str:

        print()
        print("====================================================")
        print("PartitionResolver START")
        print(f"Instrument: {instrument.value}")
        print(f"Resolution: {resolution.value}")
        print(f"Timestamp recebido: {timestamp}")
        print("====================================================")

        day_start, day_end = self._day_bounds(timestamp)

        print(f"Dia alvo start: {day_start}")
        print(f"Dia alvo end  : {day_end}")

        partitions = self.repository.find_partitions(instrument, resolution, day_start, day_end)

        if partitions:
            print("Dia ja coberto por particao existente")
            print(f"Collection encontrada: {partitions[0].collection_name}")
            return partitions[0].collection_name

        print("Nenhuma particao existente cobre o dia alvo")
        print("Iniciando criacao de nova particao")

        days_per_collection = self._calculate_days_per_collection(resolution)
        print(f"Days per collection calculado: {days_per_collection}")

        prev_p = self.repository.find_prev_partition(instrument, resolution, day_start)
        next_p = self.repository.find_next_partition(instrument, resolution, day_end)

        if prev_p:
            print(f"Prev partition encontrada: {prev_p.collection_name}")
            print(f"Prev end_date: {prev_p.end_date}")
        else:
            print("Prev partition: None")

        if next_p:
            print(f"Next partition encontrada: {next_p.collection_name}")
            print(f"Next start_date: {next_p.start_date}")
        else:
            print("Next partition: None")

        start_date, end_date = self._build_new_partition_range(
            timestamp=timestamp,
            days_per_collection=days_per_collection,
            prev_partition=prev_p,
            next_partition=next_p,
        )

        print("Resultado final do range calculado")
        print(f"Start date: {start_date}")
        print(f"End date  : {end_date}")

        if self.repository.check_overlap(instrument, resolution, start_date, end_date):
            print("ERRO FATAL: overlap detectado")
            raise Exception("Overlap detectado")

        collection_name = self._generate_collection_name(
            instrument=instrument,
            resolution=resolution,
            start_date=start_date,
            end_date=end_date,
        )

        print(f"Collection gerada: {collection_name}")

        now = datetime.utcnow()
        new_partition = ClsPartitionMapModel(
            instrument=instrument.value,
            resolution=resolution.value,
            collection_name=collection_name,
            start_date=start_date,
            end_date=end_date,
            storage_backend="MongoDB",
            status="active",
            created_at=now,
            updated_at=now,
        )

        print("Persistindo nova particao no partition_map")
        self.repository.insert_partition(new_partition)

        print("Criando collection fisica se necessario")
        self.repository.create_time_series_collection_if_not_exists(
            collection_name,
            resolution,
            instrument
        )

        print("PartitionResolver END")
        print("====================================================")
        print()

        return collection_name

    def _load_partitioning_cfg(self):
        if self._partitioning_cfg_cache is None:
            print("Carregando system_config.partitioning")
            self._partitioning_cfg_cache = ClsSystemConfigRepository.get_partitioning_config()
        return self._partitioning_cfg_cache

    @staticmethod
    def _day_bounds(ts: datetime) -> Tuple[datetime, datetime]:
        start = datetime(ts.year, ts.month, ts.day, 0, 0, 0)
        end = datetime(ts.year, ts.month, ts.day, 23, 59, 59)
        return start, end

    @staticmethod
    def _resolution_to_seconds(resolution: ClsResolutionEnum) -> float:
        v = str(resolution.value).strip().lower()

        if v.endswith("ms"):
            return int(v.replace("ms", "")) / 1000.0
        if v.endswith("s"):
            return float(v.replace("s", ""))
        if v.endswith("m"):
            return float(v.replace("m", "")) * 60.0
        if v.endswith("h"):
            return float(v.replace("h", "")) * 3600.0

        raise ValueError("Resolucao nao suportada")

    def _calculate_days_per_collection(self, resolution: ClsResolutionEnum) -> int:
        cfg = self._load_partitioning_cfg()

        target_docs = int(cfg["target_docs_per_collection"])
        sun_hours = int(cfg["sun_hours_per_day"])

        print(f"target_docs_per_collection: {target_docs}")
        print(f"sun_hours_per_day: {sun_hours}")

        seconds_per_doc = self._resolution_to_seconds(resolution)
        print(f"seconds_per_doc: {seconds_per_doc}")

        docs_per_day = (sun_hours * 3600) / seconds_per_doc
        print(f"docs_per_day estimado: {docs_per_day}")

        days = int(target_docs // docs_per_day)
        if days < 1:
            days = 1

        return days

    def _build_new_partition_range(
        self,
        timestamp: datetime,
        days_per_collection: int,
        prev_partition: Optional[ClsPartitionMapModel],
        next_partition: Optional[ClsPartitionMapModel],
    ) -> Tuple[datetime, datetime]:

        print()
        print("---- BUILD NEW PARTITION RANGE ----")

        day_start, day_end = self._day_bounds(timestamp)

        print(f"Dia alvo normalizado: {day_start}")

        cand_start, cand_end = self._epoch_canonical_range(day_start, days_per_collection)

        print("Intervalo candidato via epoch")
        print(f"Candidato start: {cand_start}")
        print(f"Candidato end  : {cand_end}")

        start_date = cand_start
        end_date = cand_end

        if prev_partition:
            prev_end = self._day_end(prev_partition.end_date)
            min_start = self._day_start(prev_end + timedelta(seconds=1))
            print(f"Recorte por prev. min_start permitido: {min_start}")

            if start_date < min_start:
                print("Start recortado pelo prev")
                start_date = min_start

        if next_partition:
            next_start = self._day_start(next_partition.start_date)
            max_end = self._day_end(next_start - timedelta(seconds=1))
            print(f"Recorte por next. max_end permitido: {max_end}")

            if end_date > max_end:
                print("End recortado pelo next")
                end_date = max_end

        print("Intervalo apos recortes")
        print(f"Final start: {start_date}")
        print(f"Final end  : {end_date}")

        if start_date > end_date:
            raise Exception("Intervalo invalido apos recorte")

        if not (start_date <= day_start <= end_date):
            raise Exception("Dia alvo fora do intervalo final")

        print("BUILD NEW PARTITION RANGE OK")
        print("----------------------------------")
        print()

        return start_date, end_date

    def _epoch_canonical_range(
        self,
        day_start: datetime,
        days_per_collection: int
    ) -> Tuple[datetime, datetime]:

        print("Calculando intervalo canonico por epoch")
        delta_days = (day_start - self.EPOCH_ANCHOR).days
        print(f"Delta days desde epoch: {delta_days}")

        window_index = delta_days // days_per_collection
        print(f"Window index: {window_index}")

        start = self.EPOCH_ANCHOR + timedelta(days=window_index * days_per_collection)
        end = start + timedelta(days=days_per_collection - 1)

        start = self._day_start(start)
        end = self._day_end(end)

        return start, end

    @staticmethod
    def _day_start(dt: datetime) -> datetime:
        return datetime(dt.year, dt.month, dt.day, 0, 0, 0)

    @staticmethod
    def _day_end(dt: datetime) -> datetime:
        return datetime(dt.year, dt.month, dt.day, 23, 59, 59)

    @staticmethod
    def _generate_collection_name(
        instrument: ClsInstrumentEnum,
        resolution: ClsResolutionEnum,
        start_date: datetime,
        end_date: datetime
    ) -> str:
        return (
            f"data_{instrument.value}_"
            f"{resolution.value}_"
            f"{start_date.strftime('%Y%m%d')}_"
            f"{end_date.strftime('%Y%m%d')}"
        )
