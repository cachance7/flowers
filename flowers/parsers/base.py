from pandas import DataFrame


class Parser:
    def __init__(self, filepath: str) -> None:
        self.filepath = filepath

    def parse(self) -> DataFrame:
        raise NotImplementedError()
