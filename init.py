import csv
import os
from disk import *
from datetime import datetime
from bitstring import *
from data import *
import json

def prepare_sorted_index(mm:BitArray):
    def create_index_value_pair(index, value, value_length):
        x = BitArray(uint = 0, length = 32 + value_length)
        x.overwrite(value, 32)
        x.overwrite(Bits(uint = index, length = 32), 0)
        
        return x
    def read_index_value_pair(index_value_pair, value_length):
        index = index_value_pair[0:32].uint
        value = index_value_pair[32:32 + value_length].uint
        return index, value
    
    file_no=0
    
    raw_data = Cursor(mm, 0, settings.MM_SIZE - settings.PAGE_SIZE)
    with open("processed_data/Year.dat", "rb") as in_f:
        eof = False
        idx = 1
        while True:
            while True:
                if raw_data.read_file(in_f):
                    continue
                else: 
                    if not raw_data.full(16):
                        eof = True
                    break
            raw_data.set_ptr(0)
            
            with open("processed_data/temp/Year{}.dat".format(file_no), "wb") as out_f:
                w_c = Cursor(mm, settings.MM_SIZE - settings.PAGE_SIZE, settings.MM_SIZE)
                w_buf = WriteBuffer(w_c, 32 + 16, out_f)
                sorter = []
                temp = raw_data.read_mm(16)
                while temp:
                    sorter.append((idx, temp))
                    idx+=1
                    temp = raw_data.read_mm(16)

                sorter.sort(key= lambda x: x[1].uint)
                raw_data.set_ptr(0)
                for i, val in sorter:
                    w_buf.write(create_index_value_pair(i, val, 16))
                    
                w_buf.close()
            file_no +=1
            if eof:
                break
    
    pos = 0
    readers = []
    values = []
    column_index = {}
    output_cur = Cursor(mm, settings.MM_SIZE - settings.PAGE_SIZE, settings.MM_SIZE)
    
    with open("processed_data/Year.idx", "wb") as out:
        output_writer = WriteBuffer(output_cur, 32, out)
        for file in sorted(os.listdir("processed_data/temp")):
            if pos * settings.PAGE_SIZE >= settings.MM_SIZE - settings.PAGE_SIZE:
                raise Exception("Not enough memory to sort index. Increase main memory size to fix.")
            
            cur = Cursor(mm, pos * settings.PAGE_SIZE, (pos + 1) *settings.PAGE_SIZE)
            cur.clear()
            readers.append(iter(ColumnReader(open("processed_data/temp/" + file, "rb"), cur, 32 + 16)))
            pos+=1
        for reader in readers:
            values.append(read_index_value_pair(next(reader), 16))
            
        while True:
            if len(values) == 0:
                break
            min_idx_val = min(values, key = lambda x: x[1])
            reader_idx = values.index(min_idx_val)
            
            min_idx = min_idx_val[0]
            min_val = min_idx_val[1]
            if min_val not in column_index.keys():
                column_index[min_val] = min_idx
            output_writer.write(Bits(uint = min_idx, length=32))
            
            try:
                temp = next(readers[reader_idx])
                if not temp:
                    values.pop(reader_idx)
                    readers[reader_idx].file.close()
                    readers.pop(reader_idx)
                    
                    continue
                else:
                    values[reader_idx] = read_index_value_pair(temp, 16)
                
            except StopIteration:
                values.pop(reader_idx)
                readers[reader_idx].file.close()
                readers.pop(reader_idx)
                
            
        output_writer.close()
        
    with open("Year_lookup.json", "w") as out:
        json.dump(column_index, out)
        
    for file in os.listdir("processed_data/temp"):
        os.remove("processed_data/temp/" + file)
    return column_index
    
if __name__ == "__main__":
    with open("SingaporeWeather.csv", "r") as in_f:
        in_csv = csv.DictReader(in_f)
        date_out = open("processed_data/Date.dat", "wb")
        station_out = open("processed_data/Station.dat", "wb")
        humid_out = open("processed_data/Humidity.dat", "wb")
        temp_out =  open("processed_data/Temperature.dat", "wb")
        year_out = open("processed_data/Year.dat", "wb")
        month_out = open("processed_data/Month.dat", "wb")
        mm = BitArray(int = 0, length = settings.MM_SIZE)
        #buffer_size = ((settings.MM_SIZE // settings.PAGE_SIZE) // 4 - 1) * settings.PAGE_SIZE 
        buffer_size =  settings.PAGE_SIZE 
        
        date_mm = Cursor(mm, 0, buffer_size)
        station_mm = Cursor(mm, buffer_size, 2* buffer_size)
        humid_mm = Cursor(mm, 2*buffer_size, 3*buffer_size)
        temp_mm = Cursor(mm, 3*buffer_size, 4*buffer_size)
        year_mm = Cursor(mm, 4*buffer_size, 5*buffer_size)
        month_mm = Cursor(mm, 5*buffer_size, 6*buffer_size)
        date_out_buf = Cursor(mm, 6*buffer_size, 7*buffer_size)
        station_out_buf = Cursor(mm, 7*buffer_size, 8*buffer_size)
        humid_out_buf = Cursor(mm, 8*buffer_size, 9*buffer_size)
        temp_out_buf = Cursor(mm, 9*buffer_size, 10*buffer_size)
        year_out_buf = Cursor(mm, 10*buffer_size, 11*buffer_size)
        month_out_buf = Cursor(mm, 11*buffer_size, 12*buffer_size)
        id_mm = Cursor(mm, 12*buffer_size, 13*buffer_size)
        id_out_buf = Cursor(mm, 13*buffer_size, 14*buffer_size)
        
        date_writer = WriteBuffer(date_out_buf, 64, date_out)
        year_writer = WriteBuffer(year_out_buf, 16, year_out)
        month_writer = WriteBuffer(month_out_buf, 4, month_out)
        station_writer = WriteBuffer(station_out_buf, 2, station_out)
        humid_writer = WriteBuffer(humid_out_buf, 32, humid_out)
        temp_writer = WriteBuffer(temp_out_buf, 16, temp_out)
        id_writer = WriteBuffer(id_out_buf, 32, open("processed_data/ID.dat", "wb"))
        i = 0
        while(True):
            if i % 1000 == 0:
                print("Processed {} rows".format(i))
                
            try:
                row = next(in_csv)
                i+=1
            except StopIteration:
                break
            
            station = row["Station"]
            date = datetime.strptime(row["Timestamp"], "%Y-%m-%d %H:%M")
            month = date.month
            year = date.year
            temperature = row["Temperature"]
            humidity = row["Humidity"]
            
            date_writer.write(to_bits(date.timestamp(), "Date"))
            year_writer.write(to_bits(year, "Year"))
            month_writer.write(to_bits(month, "Month"))
            station_writer.write(to_bits(station, "Station"))
            humid_writer.write(to_bits(humidity, "Humidity"))
            temp_writer.write(to_bits(temperature, "Temperature"))
            id_writer.write(to_bits(i, "ID"))
            
        date_writer.close()
        year_writer.close()
        month_writer.close()
        station_writer.close()
        humid_writer.close()
        temp_writer.close()
        id_writer.close()
        print("{} rows processed".format(i))
    print("Generating index sorted by year")
    mm.set(0)
    prepare_sorted_index(mm)
