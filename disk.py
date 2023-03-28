from bitstring import BitArray
import settings

        
class Cursor:
    """Accessor for the main memory, function as a partition of the main memory"""
    def __init__(self, mm: BitArray, start: int, end:int):
        """Attach to the main memory and set the start and end of the partition

        Args:
            mm (_type_): the main memory
            start (_type_): the start position of the partition
            end (_type_): the end position of the partition
        """
        self.mm = mm
        self.start = start
        self.end = end
        self.ptr = 0
    def full(self,size:int) -> bool:
        """Check if the partition is full
        """
        return self.ptr + self.start + size > self.end 
    
    def read_mm(self, size:int, loc: int = None) -> BitArray:
        """Read data from the main memory

        Args:
            size (int): Size of the data, in bits
            loc (int, optional): Position of the data within the partition. Defaults to None.

        Returns:
            BitArray: The data read from the main memory
        """        """"""
        if self.full(size):
            return None
        if loc:
            self.set_ptr(loc)
        pos = self.ptr + self.start
        
        data = self.mm[pos:pos + size]
        if data:
            self.ptr = self.ptr + size
            return data
        return None
        
    def write_mm(self, data: BitArray|bytes, loc:int = None)->bool:
        """write data to the main memory

        Args:
            data (BitArray | bytes): The data to be written
            loc (int, optional): Position to write to within the partition. Defaults to None.

        Returns:
            bool: true if the data is written successfully, false otherwise
        """        """"""
      
        if type(data) == bytes:
            size = len(data) * 8
        else:
            size = data.len
            
        if loc:
            self.set_ptr(loc)
        if self.full(size):
            return False
        
        pos = self.start + self.ptr
        
        self.mm.overwrite(data, pos)
        self.ptr = self.ptr + size
        return True
    
    def read_file(self, f, loc:int = None, mm_loc:int = None) -> bool:
        """Read data from the file and write to the main memory

        Args:
            f (_type_): The file to read from
            loc (int, optional): Position to read from within the file. Defaults to None.
            mm_loc (int, optional): Position to write to within the partition. Defaults to None.

        Returns:
            bool: True if the data is read successfully, false otherwise
        """
    
        if loc:
            f.seek(loc)
            
        b = self.file.read(settings.PAGE_SIZE_BYTES)
        self.write_mm(b, mm_loc)
    

    def write_file(self, f, size = settings.PAGE_SIZE, mm_loc = None):
        """Write data from the main memory to the file

        Args:
            f (_type_): The file to write to
            size (_type_, optional): Size of the data to write. Defaults to settings.PAGE_SIZE.
            mm_loc (_type_, optional): Position to read from within the partition. Defaults to None.

        Raises:
            ValueError: _description_
        """
        if mm_loc:
            self.set_ptr(mm_loc)
        if size > settings.PAGE_SIZE:
            raise ValueError("You cannot write more than PAGE_SIZE at a time")
        if self.full(size):
            size = self.end - mm_loc - self.start
        b = self.read_mm(size, mm_loc).bytes
        
        f.write(b)
    
    def set_ptr(self, loc = 0):
        """Set the pointer to a specific position

        Args:
            loc (int, optional): Pointer position. Defaults to 0.
        """
        self.ptr = loc
    def clear(self):
        """Set all the bits in the partition to 0
        """
        self.mm.set(False, range(self.start, self.end))
        
class ColumnReader:
    """High level representation a column of data.
    """
    def __init__(self, file, cursor:Cursor, item_size:int):
        """Initialize the column reader

        Args:
            file (_type_): Input file
            cursor (Cursor): Cursor object to store the data within the main memory
            item_size (int): Size of each item in the column, in bits
        """
        self.cur = cursor
        self.file = file
        self.item_per_page = settings.PAGE_SIZE / item_size
        
        
    def __iter__(self):
        """Iterate over the length of the cursor.
        You should fill the memory with data from sequential reading before iterating.

        Returns:
            _type_: _description_
        """
        self.cur.set_ptr(0)
        return self
        
    def __next__(self):
        """Get next item in the column

        Raises:
            StopIteration: _description_

        Returns:
            _type_: _description_
        """
        temp = self.cur.read_mm(self.item_size)
        if temp:
            return temp
        else:
            raise StopIteration
    
    def sequential_read(self):
        """Fills the main memory with data from the file. 
        """
        while self.cur.read_file(self.file):
            pass
        
        
    def random_read(self, index):
        """Read a specific item from the column file.

        Args:
            index (_type_): Index of the item to read

        Returns:
            _type_: The item to be read
        """
        file_loc = index * self.item_size // (settings.PAGE_SIZE)
        page_loc = index * self.item_size // (settings.PAGE_SIZE)
        if not self.cur.read_file(self.file, file_loc * settings.PAGE_SIZE_BYTES):
            self.cur.set_ptr(0)
            self.cur.read_file(self.file, file_loc * settings.PAGE_SIZE_BYTES)
        return self.cur.read_mm(self.item_size, self.cur.ptr - settings.PAGE_SIZE + page_loc)


        
        
    
class ColumnWriter:
    """Class for writing to a column file
    """
    def __init__(self, cursor, size):
        self.cur = cursor
        self.item_per_page = settings.PAGE_SIZE / size
        
    def write(self, data):
        """Write an item to the main memory (within the cursor).
        When the cursor is full, flush to the file.

        Args:
            data (_type_): The item to be written

        Returns:
            _type_: True if the item is written successfully, false otherwise
        """
        if self.cur.write_mm(data):
            return True
        else:
            self.cur.set_ptr(0)
            while self.cur.write_file(self.file):
                pass
            self.cur.clear()
            self.cur.set_ptr(0)
            self.cur.write_mm(data)
            
    def flush(self):
        """Flush the remaining data to the file.
        Always run this after fully writing all the data for the column to this class
        """
        temp = self.cur.ptr
        self.cur.set_ptr(0)
        while True:
            if not self.cur.write_file(self.file):
                break
            temp -= settings.PAGE_SIZE
            if temp < settings.PAGE_SIZE:
                break
        self.cur.write.file(self.file, temp)
            
        
        
        
            
    
        
        
        
class MainMemory:
    def __init__(self, file = None):
        self.mm = BitArray(length = settings.MM_SIZE)
        self.mm_ptr = 0
        self.file_locations = {
            "Date": "processed_data/Date.dat",
            "Station": "processed_data/Date.dat",
            "Humidity": "processed_data/Humidity.dat",
            "Temperature": "processed_data/Temperature.dat"
        }


    
    
if __name__ == "__main__":
    pass