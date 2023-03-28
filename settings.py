PAGE_SIZE = 400  # bits (Select a multiple of 8)
PAGE_SIZE_BYTES = PAGE_SIZE / 8
MM_SIZE = PAGE_SIZE * 10
DATA_DIR = "./processed_data"

EMPTY = "0"
MISSING = "1"

if PAGE_SIZE % 8 != 0:
    raise ValueError("Disk page size should be a multiple of 8")