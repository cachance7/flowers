import camelot
import pandas as pd
import logging

from .base import Parser

log = logging.getLogger(__name__)


# The column names found in each cleaned frame
PRODUCT_COL = "product"
PRICE_COL = "price_each"
GROWER_COL = "grower"
LOCATION_COL = "location"
STEMS_COL = "stems"
DAYS_COL = "delivery_days"
AVAILIBILITY_COL = "availability"

COLUMNS = [
    PRODUCT_COL,
    PRICE_COL,
    GROWER_COL,
    LOCATION_COL,
    STEMS_COL,
    DAYS_COL,
    AVAILIBILITY_COL,
]

REMAPPED_COLUMNS = {
    "typical week delivery day(s)": DAYS_COL,
    "typical delivery day(s)": DAYS_COL,
    "availibility": AVAILIBILITY_COL,
}

# Initial tables
# These are really just product types and so we’ll concat them all at the end
# with a column to represent this initial splitting.
FLOWERS_KEY = "FLOWERS"
TEXTURE_KEY = "TEXTURE"
FOLIAGE_KEY = "FOLIAGE"
BLOOMING_BRANCHES_KEY = "BLOOMING BRANCHES"
DRIED_FLOWERS_KEY = "DRIED FLOWERS"

# Sentinel for end of final table
END_KEY = "Pricing and Availibility"


# Typical order (but do not assume!)
TABLE_KEYS = [
    FLOWERS_KEY,
    TEXTURE_KEY,
    FOLIAGE_KEY,
    BLOOMING_BRANCHES_KEY,
    DRIED_FLOWERS_KEY,
    END_KEY,
]


def remove_newline(val: str) -> str:
    return val.replace("\n", "")


def lowercase(val: str):
    return val.lower()


def trim(val: str):
    return val.strip()


def clean_cell(val: str):
    return flow(val, remove_newline, lowercase, trim)


def flow(seed, *funcs):
    for func in funcs:
        seed = func(seed)
    return seed


def clean_frame(frame: pd.DataFrame):
    frame = frame.copy()
    frame.reset_index(inplace=True)

    # Get rid of unnecessary columns
    frame.pop("index")
    frame.pop("level_0")

    # TODO Maybe don’t ignore the errors?
    # pd.options.mode.chained_assignment = None

    # Clean up the weird values in each row
    for i, _ in enumerate(frame.columns):
        frame[i] = frame[i].map(clean_cell)

    # pd.options.mode.chained_assignment = 'warn'

    # Assign column names to values of first row
    frame.columns = frame.iloc[0]

    # Drop first row now that we have good column names
    frame = frame[1:]

    return frame


class SwgmcParser(Parser):
    def __init__(self, filepath: str) -> None:
        super().__init__(filepath)
        self.tables = TABLE_KEYS

    def parse(self):
        lookups = {}

        tables = camelot.read_pdf(self.filepath, pages="1-end")

        df = pd.concat([t.df for t in tables])

        df.reset_index(inplace=True)

        for key in self.tables:
            try:
                lookups[key] = df[df[0].str.contains(key)].index[0]
            except IndexError:
                log.warning(f"key not found: {key}")

        sorted_indexes = sorted(lookups.items(), key=lambda x: x[1])
        frames: dict[str, pd.DataFrame] = {}
        for i, _ in enumerate(sorted_indexes):
            if i == len(sorted_indexes) - 1:
                break

            key, start_index = sorted_indexes[i]
            _, end_index = sorted_indexes[i + 1]
            frames[key] = df[start_index + 1 : end_index]

        cleaned_frames = []
        for key, frame in frames.items():
            cleaned_frame = clean_frame(frame)
            cleaned_frame["type"] = key.lower()
            cleaned_frame.rename(columns=REMAPPED_COLUMNS, inplace=True)
            cleaned_frames.append(cleaned_frame)

        return pd.concat(cleaned_frames).reset_index(drop=True)
