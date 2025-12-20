# src/enums/ClsMongoScopeEnum.py

from enum import Enum

class ClsMongoScopeEnum(str, Enum):
    MASTER = "MASTER"
    INSTRUMENT = "INSTRUMENT"
    PORTAL = "PORTAL"
