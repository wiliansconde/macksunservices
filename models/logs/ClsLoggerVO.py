from enums.ProcessStatus import ProcessStatus
from utils.ClsGet import ClsGet


class ClsLoggerVO:
    def __init__(self, file_path, user, status=ProcessStatus.PENDING, actions=None):
        self.file_path = file_path
        self.user = user
        self.created_on = ClsGet.current_time()
        self.updated_on = None
        self.status = status
        self.actions = actions or []

    def to_dict(self):
        return {
            'FILEPATH': self.file_path,
            'USER': self.user,
            'CREATED_ON': self.created_on,
            'UPDATED_ON': self.updated_on,
            'STATUS': self.status,
            'ACTIONS': self.actions
        }
