from bitstring import Bits
import settings
from datetime import datetime

def to_bits(data, type):
    
    if type == "Station":
       if data == "Changi":
           return Bits(bin = "10")
       elif data == "M":
           return Bits(bin = "11")
       elif data == "Paya Lebar":
           return Bits(bin = "01")
       
       
    elif type =="Humidity":
        if data == "M":
            return Bits(bin = "1" * 32)
        else:
            data = float(data)
            if data == 0:
                return Bits(float = -0, length = 32)
            
            return Bits(float = data, length = 32)
    
    elif type == "Temperature":
        
        if data == "M":
            return Bits(bin = "1" * 16)
        else:
            data = int(float(data))
            if data == 0:
                return Bits(int = -0, length = 16)
            return Bits(int = data, length = 16)
    elif type == "Date":
        if data == "M":
            return Bits(bin = "1" * 64)
        else:
            data = float(data)
            if data == 0:
                return Bits(float = -0, length = 64)
            
            return Bits(float = data, length = 64)
    elif type == "Year":
        if data == "M":
            return Bits(bin = "1" * 16)
        else: 
            return Bits(uint = int(data), length = 16)
    elif type == "Month":
        if data == "M":
            return Bits(bin = "1" * 4)
        else:
            return Bits(uint = int(data), length = 4)
    elif type == "ID":
        return Bits(uint = data, length = 32)

def from_bits(data, type):
    if type == "Date":
        if data.float == -0:
            return 0
        if data.bin == "1" * 64:
            return "M"
        return datetime.fromtimestamp(data.float).strftime("%Y-%m-%d %H:%M")
    elif type == "Year":
        if data.bin == "1" * 16:
            return "M"
        return data.uint
    elif type == "Month":
        if data.bin == "1" * 4:
            return "M"
        return data.uint
    elif type == "Station":
       if data.bin == "10":
           return "Changi"
       elif data.bin == "01":
           return "Paya Lebar"
       elif data.bin == "11":
           return "M"

    elif type == "Humidity":
        if data.float == -0:
            return 0
        elif data.bin == "1" * 32:
            return "M"
        return data.float
    elif type == "Temperature":
        if data.int == -0:
            return 0
        elif data.bin == "1" * 16:
            return "M"
        return data.int