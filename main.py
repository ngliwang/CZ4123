import sys
import numpy as np
from bitstring import *
from datetime import datetime
s = BitArray(length = 10)
from disk import *
from init import *
if __name__ == "__main__":

    # make a temp folder at processed_data if it does not exist
    if not os.path.exists("processed_data/temp"):
        os.makedirs("processed_data/temp")
  
    output = {}
    mm = BitArray(length = settings.MM_SIZE)
    year_reader = RandomColumnReader(open("processed_data/Year.dat", "rb"), Cursor(mm, 0, settings.MM_SIZE - settings.PAGE_SIZE), 16)
    matric = "U1921246F"
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
                    print("Reading position: ", year_pos.uint, "\t|station : " , station)
                    if station == station_query:
                        station_pos_writer.write(year_pos)
                station_pos_writer.close()
    
    min_temp = {}
    max_temp = {}
    min_humid = {}
    max_humid = {}
    for y in year_query:
        for i in range(1, 13):
            min_temp[(int(y), i)] = 100
            max_temp[(int(y), i)] = -100
            min_humid[(int(y), i)] = 100
            max_humid[(int(y), i)] = -100
            
    mm.set(0)
    with open("processed_data/temp/Station.pos", "rb") as station_pos_f:
        month_f = open("processed_data/Month.dat", "rb")
        temperature_f = open("processed_data/Temperature.dat", "rb")
        humidity_f = open("processed_data/Humidity.dat", "rb")
        year_f = open("processed_data/Year.dat", "rb")
        buffer_size = settings.MM_SIZE // 5 - settings.PAGE_SIZE
        station_pos_reader = ColumnReader(station_pos_f, Cursor(mm, 0, 1*buffer_size), 32)
        month_reader = RandomColumnReader(month_f, Cursor(mm, 1*buffer_size, 2*buffer_size), 4)
        temperature_reader = RandomColumnReader(temperature_f, Cursor(mm, 2*buffer_size, 3*buffer_size), 16)
        humidity_reader = RandomColumnReader(humidity_f, Cursor(mm, 3*buffer_size, 4*buffer_size), 32)
        year_reader = RandomColumnReader(year_f, Cursor(mm, 4*buffer_size, 5*buffer_size), 16)
        for pos in station_pos_reader:
            month = from_bits(month_reader[pos.uint - 1], "Month")
            temperature = from_bits(temperature_reader[pos.uint - 1], "Temperature")
            humidity = from_bits(humidity_reader[pos.uint - 1], "Humidity")
            year = from_bits(year_reader[pos.uint - 1], "Year")
            if (year, month) not in min_temp.keys():
                continue
            else:
                if temperature != "M":
                    if temperature < min_temp[(year, month)]:
                        min_temp[(year, month)] = temperature
                        print("At position: " , pos.uint, "\tDetected new min temp: ", temperature, " for month: ", month, "/", year)
                    if temperature > max_temp[(year, month)]:
                        max_temp[(year, month)] = temperature
                        print("At position: " , pos.uint, "\tDetected new max temp: ", temperature, " for month: ", month, "/", year)
                if humidity != "M":
                    if humidity < min_humid[(year, month)]:
                        min_humid[(year, month)] = humidity
                        print("At position: " , pos.uint, "\tDetected new min humid: ", humidity, " for month: ", month, "/", year)
                    if humidity > max_humid[(year, month)]:
                        max_humid[(year, month)] = humidity
                        print("At position: " , pos.uint, "\tDetected new max humid: ", humidity, " for month: ", month, "/", year)
        month_f.close()
        temperature_f.close()   
        humidity_f.close()
    print("Year\tMonth\tMin Temp\tMax Temp\tMin Humid\tMax Humid")
    for year in year_query:
        for i in range(1, 13):
            print(year, i, min_temp[(int(year), i)], max_temp[(int(year), i)], min_humid[(int(year), i)], max_humid[(int(year), i)], sep = "\t")
        
    mm.set(0)
    
    print("Writing to file...")
    with open("results/ScanResult_{}.csv".format(matric), "w") as f:
        buffer_size = settings.MM_SIZE // 8
        with open("processed_data/temp/Station.pos", "rb") as station_pos_f:
            month_f = open("processed_data/Month.dat", "rb")
            temperature_f = open("processed_data/Temperature.dat", "rb")
            humidity_f = open("processed_data/Humidity.dat", "rb")
            year_f = open("processed_data/Year.dat", "rb")
            date_reader = RandomColumnReader(open("processed_data/Date.dat", "rb"), Cursor(mm, 0, buffer_size), 64)
            month_reader = RandomColumnReader(month_f, Cursor(mm, 1*buffer_size, 2*buffer_size), 4)
            temperature_reader = RandomColumnReader(temperature_f, Cursor(mm, 2*buffer_size, 3*buffer_size), 16)
            humidity_reader = RandomColumnReader(humidity_f, Cursor(mm, 3*buffer_size, 4*buffer_size), 32)
            station_pos_reader = ColumnReader(station_pos_f, Cursor(mm, 4*buffer_size, 5*buffer_size), 32)
            year_reader = RandomColumnReader(year_f, Cursor(mm, 5*buffer_size, 6*buffer_size), 16)
            
            f.write("Date,Station,Category,Value\n")
            for pos in station_pos_reader:
                month = from_bits(month_reader[pos.uint - 1], "Month")
                temperature = from_bits(temperature_reader[pos.uint - 1], "Temperature")
                humidity = from_bits(humidity_reader[pos.uint - 1], "Humidity")
                year = from_bits(year_reader[pos.uint - 1], "Year")
                
                if month in range(1, 13) and str(year) in year_query:
                    if temperature == min_temp[(year,month)]:
                        f.write("{},{},{},{}\n".format(from_bits(date_reader[pos.uint - 1], "Date")[:10], station_query, "Min Temperature", temperature))
                    if temperature == max_temp[(year,month)]:
                        f.write("{},{},{},{}\n".format(from_bits(date_reader[pos.uint - 1], "Date")[:10], station_query, "Max Temperature", temperature))
                    if humidity != "M" and humidity == min_humid[(year,month)]:    
                        f.write("{},{},{},{}\n".format(from_bits(date_reader[pos.uint - 1], "Date")[:10], station_query, "Min Humidity", round(humidity,2)))
                    if humidity != "M" and humidity == max_humid[(year,month)]:
                        f.write("{},{},{},{}\n".format(from_bits(date_reader[pos.uint - 1], "Date")[:10], station_query, "Max Humidity", round(humidity,2)))
    
    # Cursor.remove(matric)
    mm.set(0)
    os.remove("processed_data/temp/Year.pos")
    os.remove("processed_data/temp/Station.pos")
    

                        
                        
    