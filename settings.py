PAGE_SIZE = 400  # bits (Select a multiple of 8)
PAGE_SIZE_BYTES = PAGE_SIZE / 8
DISK_CACHE_MULTIPLIER = 10
DISK_CACHE_SIZE = PAGE_SIZE * DISK_CACHE_MULTIPLIER
DATA_DIR = "./processed_data"

EMPTY = "0"
MISSING = "1"

if PAGE_SIZE % 8 != 0:
    raise ValueError("Disk page size should be a multiple of 8")