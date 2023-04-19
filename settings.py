PAGE_SIZE = 7680 # bits (Select a multiple of 8)
PAGE_SIZE_BYTES = PAGE_SIZE // 8
MM_SIZE = PAGE_SIZE * 200
DATA_DIR = "./processed_data"
if PAGE_SIZE % 8 != 0:
    raise ValueError("Disk page size should be a multiple of 8")

if __name__ == "__main__":
    print(int("M"))