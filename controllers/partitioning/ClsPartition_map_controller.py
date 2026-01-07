from datetime import datetime
from typing import List

from enums.ClsInstrumentEnum import ClsInstrumentEnum
from enums.ClsResolutionEnum import ClsResolutionEnum
from services.ClsDataPartitionResolverService import ClsDataPartitionResolverService


class ClsPartitionMapController:

    def __init__(self):
        self.resolver_service = ClsDataPartitionResolverService()

    def get_target_collection(self, instrument: ClsInstrumentEnum, resolution: ClsResolutionEnum, timestamp: datetime) -> str:
        print('+++++++++++++++++++++++++++++++++++++++++++++++++++++')
        print('ClsPartitionMapController - descobrindo a collection')
        print('ClsPartitionMapController - descobrindo a collection')
        print('ClsPartitionMapController - descobrindo a collection')
        print('ClsPartitionMapController - descobrindo a collection')
        return self.resolver_service.get_target_collection(instrument, resolution, timestamp)

