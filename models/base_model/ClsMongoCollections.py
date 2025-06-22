import os
import os

class ClsMongoCollections:
    FILE_INGESTION_QUEUE = 'file_ingestion_queue'
    PROCESSED_FILE_TRACE = 'processed_file_trace'
    DATA_SST_RF_FILE_05MS: str = 'data_SST_rf_file_05ms'
    DATA_SST_RS_FILE_40MS = 'data_SST_rs_file_40ms'
    DATA_SST_BI_FILE_1S = 'data_SST_bi_file_1s'
    DATA_POEMAS_FILE_10ms = 'data_POEMAS_file_10ms'

    @staticmethod
    def get_collection_name(file_path: str) -> str:
        # Extract the prefix from the file_path
        file_name = os.path.basename(file_path)
        prefix = file_name[:2]  # Assuming the prefix is the first two characters of the file name
        extension = os.path.splitext(file_name)[1]  # Get the file extension

        if extension == ".trk":
            return ClsMongoCollections.DATA_POEMAS_FILE_10ms
        elif prefix == "rf":
            return ClsMongoCollections.DATA_SST_RF_FILE_05MS
        elif prefix == "rs":
            return ClsMongoCollections.DATA_SST_RS_FILE_40MS
        elif prefix == "bi":
            return ClsMongoCollections.DATA_SST_BI_FILE_1S
        else:
            raise ValueError("Unsupported prefix or extension in file_path: {}".format(file_path))
