import camelot
import pandas as pd
import logging

log = logging.getLogger(__name__)

# TODO: remove this
file = "./swgmc_2023-03-13.pdf"

# The column names found in each cleaned frame
PRODUCT_COL = "Product"
PRICE_COL = "Price Each"
GROWER_COL = "Grower"
LOCATION_COL = "Location"
STEMS_COL = "STEMS"
DAYS_COL = "TYPICAL DELIVERY DAY(S)"
AVAILIBILITY_COL = "Availibility"

# The expected tables
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


def clean_frame(frame):
    frame.reset_index(inplace=True)

    # Get rid of unnecessary columns
    frame.pop("index")
    frame.pop("level_0")

    # TODO Maybe don’t ignore the errors?
    pd.options.mode.chained_assignment = None

    # Clean up the weird values in each row
    for i, _ in enumerate(frame.columns):
        frame[i] = frame[i].map(remove_newline)

    pd.options.mode.chained_assignment = 'warn'


    # Assign column names to values of first row
    frame.columns = frame.iloc[0]

    # Drop first row now that we have good column names
    frame = frame[1:]

    return frame


def extract_frames(filepath, tables=TABLE_KEYS):
    lookups = {}

    tables = camelot.read_pdf(filepath, pages="1-end")

    df = pd.concat([t.df for t in tables])

    df.reset_index(inplace=True)

    for key in TABLE_KEYS:
        try:
            lookups[key] = df[df[0].str.contains(key)].index[0]
        except IndexError:
            log.warning(f"key not found: {key}")

    sorted_indexes = sorted(lookups.items(), key=lambda x: x[1])
    frames = {}
    for i, _ in enumerate(sorted_indexes):
        if i == len(sorted_indexes) - 1:
            break

        key, start_index = sorted_indexes[i]
        _, end_index = sorted_indexes[i + 1]
        frames[key] = df[start_index + 1 : end_index]

    for key, frame in frames.items():
        frames[key] = clean_frame(frame)

    return frames


if __name__ == "__main__":
    frames = extract_frames(file)
