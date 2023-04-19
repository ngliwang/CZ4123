import sys
import numpy as np
from bitstring import *
from datetime import datetime
s = BitArray(length = 10)
from disk import *
from init import *
if __name__ == "__main__":
  
    output = {}
    mm = BitArray(length = settings.MM_SIZE)
    matric = "U2021106D"
    with open("Year_lookup.json", "r") as f:
        lookup = json.load(f)
    year_query = []
    for k in lookup.keys():
        if k.endswith(matric[7]):
            year_query.append(k)
    
    assert len(year_query) == 2
    station_query = int(matric[6])
    if station_query % 2 == 0:
        station_query = "Changi"
    else:
        station_query = "Paya Lebar"
        
    print("Querying for year: ", year_query)
    print("Querying for station: ", station_query)
    
    with open("processed_data/Year.idx", "rb") as year_idx_f:
        idx_reader = RandomColumnReader(year_idx_f, Cursor(mm, 0, settings.MM_SIZE - settings.PAGE_SIZE), 32)
        with open("processed_data/temp/Year.pos", "wb") as year_pos_f:
            year_pos_writer = WriteBuffer(Cursor(mm, settings.MM_SIZE - settings.PAGE_SIZE, settings.MM_SIZE), 32, year_pos_f)
            for year in year_query:
                if str(int(year)+1) in lookup.keys():
                    start = lookup[year]
                    end = lookup[str(int(year)+1)]
                    for i in range(start, end):
                        year_pos_writer.write(idx_reader[i-1])
                        print("Writing position: ", idx_reader[i-1].uint, " for year: ", year, " at index: ", i-1, "")
                else:
                    i = lookup[year]
                    while True:
                        temp = idx_reader[i-1]
                        i+=1
                        if temp:
                            year_pos_writer.write(temp)
                        else:
                            break 
            year_pos_writer.close()
    mm.set(0)
    with open("processed_data/temp/Year.pos", "rb") as year_pos_f:
        with open("processed_data/Station.dat", "rb") as station_f:
            with open("processed_data/temp/Station.pos", "wb") as station_pos_f:
                buffer_size = settings.MM_SIZE//2 - settings.PAGE_SIZE
                year_pos_reader = ColumnReader(year_pos_f, Cursor(mm, 0, buffer_size), 32)
                station_reader = RandomColumnReader(station_f, Cursor(mm, buffer_size, 2*buffer_size), 2)
                station_pos_writer = WriteBuffer(Cursor(mm, settings.MM_SIZE - settings.PAGE_SIZE, settings.MM_SIZE), 32, station_pos_f)
                for year_pos in year_pos_reader:
                    station = from_bits(station_reader[year_pos.uint - 1], "Station")
                    print("Reading position: ", year_pos.uint, " in station column: " , station)
                    if station == station_query:
                        station_pos_writer.write(year_pos)
                        
                station_pos_writer.close()                    
    min_temp = {}
    max_temp = {}
    min_humid = {}
    max_humid = {}
    for i in range(1, 13):
        min_temp[i] = (None, 100, None)
        max_temp[i] = (None, -100, None)
        min_humid[i] = (None, None, 100)
        max_humid[i] = (None, None, -100)
    mm.set(0)
    with open("processed_data/temp/Station.pos", "rb") as station_pos_f:
        month_f = open("processed_data/Month.dat", "rb")
        temperature_f = open("processed_data/Temperature.dat", "rb")
        humidity_f = open("processed_data/Humidity.dat", "rb")
        buffer_size = settings.MM_SIZE // 4 - settings.PAGE_SIZE
        station_pos_reader = ColumnReader(station_pos_f, Cursor(mm, 0, 1*buffer_size), 32)
        month_reader = RandomColumnReader(month_f, Cursor(mm, 1*buffer_size, 2*buffer_size), 4)
        temperature_reader = RandomColumnReader(temperature_f, Cursor(mm, 2*buffer_size, 3*buffer_size), 16)
        humidity_reader = RandomColumnReader(humidity_f, Cursor(mm, 3*buffer_size, 4*buffer_size), 32)
        for pos in station_pos_reader:
            month = from_bits(month_reader[pos.uint - 1], "Month")
            temperature = from_bits(temperature_reader[pos.uint - 1], "Temperature")
            humidity = from_bits(humidity_reader[pos.uint - 1], "Humidity")
            print("Read position: ", pos.uint, " for month: ", month, " temperature: ", temperature, " humidity: ", humidity)
            out = (pos.uint, temperature, humidity)
            if month not in min_temp.keys():
                continue
            else:
                if temperature != "M":
                    if temperature < min_temp[month][1]:
                        min_temp[month] = out
                    if temperature > max_temp[month][1]:
                        max_temp[month] = out
                if humidity != "M":
                    if humidity < min_humid[month][2]:
                        min_humid[month] = out
                    if humidity > max_humid[month][2]:
                        max_humid[month] = out
        month_f.close()
        temperature_f.close()   
        humidity_f.close()
    print("Month\tMin Temp\tMax Temp\tMin Humid\tMax Humid")
    for i in range(1, 13):
        print(i, min_temp[i][1], max_temp[i][1], min_humid[i][2], max_humid[i][2], sep = "\t")
    
    mm.set(0)
    date_reader = RandomColumnReader(open("processed_data/Date.dat", "rb"), Cursor(mm, 0, settings.MM_SIZE), 64)
    print("Writing to file...")
    with open("results/ScanResult_{}.csv".format(matric), "w") as f:
        f.write("Date,Station,Category,Value\n")
        for i in range(1, 13):
            f.write("{},{},{},{}\n".format(from_bits(date_reader[min_temp[i][0] - 1], "Date"), station_query, "Min Temperature", min_temp[i][1]))
            f.write("{},{},{},{}\n".format(from_bits(date_reader[max_temp[i][0] - 1], "Date"), station_query, "Max Temperature", max_temp[i][1])) 
            f.write("{},{},{},{}\n".format(from_bits(date_reader[min_humid[i][0] - 1], "Date"), station_query, "Min Humidity", min_humid[i][2]))
            f.write("{},{},{},{}\n".format(from_bits(date_reader[max_humid[i][0] - 1], "Date"), station_query, "Max Humidity", max_humid[i][2]))
    mm.set(0)
    os.remove("processed_data/temp/Year.pos")
    os.remove("processed_data/temp/Station.pos")
    

                        
                        
    