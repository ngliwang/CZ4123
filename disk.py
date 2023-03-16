from bitstring import BitArray
import settings

class Disk:
    def __init__(self, lookup):
        self.disk_cache = BitArray(settings.DISK_CACHE_SIZE)
        self.page = BitArray(settings.PAGE_SIZE)
        self.disk_ptr = 0
        self.file_ptr = 0
        self.file = ""
        
    def full(self,size ):
        return self.disk_ptr + size >= settings.DISK_CACHE_SIZE 
    
    def read_cache(self, size, loc = None):
        if self.full(size):
            return None
        if loc:
            self.set_disk_ptr(loc)
        data = self.disk_cache[self.disk_ptr:self.disk_ptr + size]
        if data:
            self.disk_ptr = self.disk_ptr + size
            return data
        return None
        
    def write_cache(self, data, loc = None):
        size = data.len
        if self.full(size):
            return False
        self.disk_cache.overwrite(data, self.disk_ptr)
        self.disk_ptr = self.disk_ptr + size
        return True
    
    def read_file(self, loc = None):
        if loc:
            self.set_file_ptr(loc)
        self.page.overwrite()
            
        

        
    def set_file(self, f):
        self.file = f
            
    def set_disk_ptr(self, loc = 0):
        self.disk_ptr = loc
        
    def set_file_ptr(self, loc = 0):
        self.file_ptr = loc
    