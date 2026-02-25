import ulid
from datetime import datetime


class LogSequenceIssuer:
    def next_sequence(self):
        return f"{ulid.ULID()}"

    def sequence_datetime(self, lsn: str):
        dt: datetime = ulid.ULID.from_str(lsn).datetime
        notz = dt.replace(tzinfo=None)
        return notz.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


if __name__ == "__main__":
    lsi = LogSequenceIssuer()
    print(lsi.sequence_datetime(lsi.next_sequence()))
