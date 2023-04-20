from bitstring import BitArray
import settings
from typing import Union
import os
import csv
        
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
        if loc != None:
            self.set_ptr(loc)
        if self.full(size):
            return None

        pos = self.ptr + self.start
        
        data = self.mm[pos:pos + size]
        if data:
            self.ptr = self.ptr + size
            return data
        return None
        
    def write_mm(self, data: Union[BitArray, bytes], loc:int = None)->bool:
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
            
        if loc != None:
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
    
        if loc != None:
            f.seek(loc)
        
        temp = self.ptr
        
        if mm_loc != None:
            self.set_ptr(mm_loc)
            
        if self.full(settings.PAGE_SIZE_BYTES):
            self.ptr = temp
            return False
        b = f.read(settings.PAGE_SIZE_BYTES)
        if not b:
            return False
        return self.write_mm(b, mm_loc)
    

    def write_file(self, f, size = settings.PAGE_SIZE, mm_loc = None):
        """Write data from the main memory to the file

        Args:
            f (_type_): The file to write to
            size (_type_, optional): Size of the data to write. Defaults to settings.PAGE_SIZE.
            mm_loc (_type_, optional): Position to read from within the partition. Defaults to None.

        Raises:
            ValueError: _description_
        """
        
            
        if mm_loc!= None:
            self.set_ptr(mm_loc)
        if size > settings.PAGE_SIZE:
            raise ValueError("You cannot write more than PAGE_SIZE at a time")
        if self.full(size):
            size = self.end - self.ptr - self.start
            if size == 0:
                return False
            else: raise ValueError("Buffer size mismatch!!!")
        b = self.read_mm(size, mm_loc)
        if not b:
            return False
        f.write(b.bytes)
        return True
            
    
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

    def remove(matric):
        input_file = "results/ScanResult_{}.csv".format(matric)
        output_file = "results/ScanResult_{}.csv".format(matric)

        if not os.path.exists("results"):
            os.makedirs("results")

        unique_rows = set()

        with open(input_file, "r") as input_csv:
            reader = csv.reader(input_csv)
            header = next(reader)

            for row in reader:
                unique_rows.add(tuple(row))

        with open(output_file, "w", newline="") as output_csv:
            writer = csv.writer(output_csv)
            writer.writerow(header)
            writer.writerows(unique_rows)

    
        
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
        self.item_size = item_size
        self.item_per_page = settings.PAGE_SIZE / item_size
        self.last_batch = False
        
        
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
            if not self.last_batch:
                self.last_batch =self.load()
                self.cur.set_ptr(0)
                return self.cur.read_mm(self.item_size)
            else:
                raise StopIteration
    
    def load(self):
        """Fills the main memory with data from the file. 
        """
        self.cur.clear()
        self.cur.set_ptr(0)
        while True:
            if self.cur.read_file(self.file):
                continue
            else:
                if not self.cur.full(settings.PAGE_SIZE):
                    
                    return True
                else:
                    return False

    
class RandomColumnReader():
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
        self.current_loaded_pages = {}
        self.item_size = item_size
        self.in_mm_page_pos = 0
        self.num_pages_in_mm =  (self.cur.end - self.cur.start) // settings.PAGE_SIZE
        
    def load_page(self, page_number):
        if self.in_mm_page_pos >= self.num_pages_in_mm:
            self.in_mm_page_pos = 0
        if self.cur.read_file(self.file, page_number * settings.PAGE_SIZE_BYTES, self.in_mm_page_pos * settings.PAGE_SIZE):
            temp = self.in_mm_page_pos
            self.current_loaded_pages[self.in_mm_page_pos] = page_number
            self.cur.ptr = self.in_mm_page_pos * settings.PAGE_SIZE
            self.in_mm_page_pos +=1
            return temp
        return None
    def __getitem__(self, index):

        page_number = index * self.item_size // (settings.PAGE_SIZE)
        pos_in_page = index * self.item_size % (settings.PAGE_SIZE)
        for i in self.current_loaded_pages:
            if self.current_loaded_pages[i] == page_number:
                return self.cur.read_mm(self.item_size, i * settings.PAGE_SIZE + pos_in_page)
        new_page_loc = self.load_page(page_number)
        if new_page_loc != None:
            return self.cur.read_mm(self.item_size, new_page_loc * settings.PAGE_SIZE + pos_in_page)
        return None

            
class WriteBuffer:
    """Class for writing to a column file
    """
    def __init__(self, cur:Cursor, size, out_file):
        self.file = out_file
        self.cur = cur
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
            return self.cur.write_mm(data)
        
    def close(self):
        """Flush the remaining data to the file.
        Always run this after fully writing all the data for the column to this class
        """
        
        self.cur.set_ptr(0)
        while self.cur.write_file(self.file):
            pass
        self.cur.clear()
        self.cur.set_ptr(0)
        self.file.close()

            

        

    
if __name__ == "__main__":
    pass