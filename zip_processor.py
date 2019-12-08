from io import BytesIO
from zipfile import ZipFile
import pandas as pd
from io import StringIO
import database


def processZip(file):
    columns = ["date", "time", "charge_point_id", "charge_point_type", "status", "coordinates", "address",
               "longitude", "latitude"]
    with ZipFile(BytesIO(file)) as my_zip_file:
        for contained_file in my_zip_file.namelist():
            df = pd.read_csv(my_zip_file.open(contained_file), sep="\t", header=None, encoding='cp1252', names=columns,
                             dtype={'date': 'str', 'time': 'str'})
            normalizeData(df)
            storeData(df)

            # Performance not great for this alternative method
            # np.vectorize(storeData)(df['date'], df['time'], df['charge_point_id'], df['charge_point_type'],
            #                         df['status'], df['address'], df['longitude'], df['latitude'])


def normalizeData(df):
    # Remove rows without latitude and longitude
    df.drop(df[(df.longitude == 0) | (df.latitude == 0)].index, inplace=True)

    # Transform date and time columns to a datetime format, so no conversion needed to insert in the proper tables
    df['datetime'] = pd.to_datetime((df['date'] + df['time']), format="%Y%m%d%H%M", errors='coerce')

    # This is needed because one of the files had some rows malformed
    # and the conversion of the date was crashing the parse
    df.dropna(subset=['datetime'], inplace=True)


def storeData(df):
    bulkInsert(df)  # Insert the whole df into a temporary table
    database.moveDataFromTemporaryTable()  # Move data from the temporary table to the proper tables
    database.truncateBulkInsertTable()  # Clean temporary table


def bulkInsert(df):
    sio = StringIO()  # Create file-like in memory for copy method
    sio.write(df.to_csv(index=None, header=None, sep='|'))  # Write the Pandas DataFrame as a csv to the buffer
    sio.seek(0)  # Be sure to reset the position to the start of the stream
    database.bulkInsert(sio, df.columns)  # Insert full df into Postgresql temporary table
    sio.close()  # Clear memory buffer

# Performance of this alternative method was not great
#
# def storeData(date, time, charge_point_id, charge_point_type, status, address, longitude, latitude):
#     chargePoint, date, status = transformData(date, time, charge_point_id, charge_point_type,
#                                               status, address, longitude, latitude)
#     database.insertEVData(chargePoint, date, status)
#
#
# def transformData(date, time, charge_point_id, charge_point_type, status, address, longitude, latitude):
#     chargePoint = getChargePoint(charge_point_id, latitude, longitude, address, charge_point_type)
#     date = getDate(date, time)
#     status = status
#     return chargePoint, date, status
#
#
# def getChargePoint(charge_point_id, latitude, longitude, address, charge_point_type):
#     return {
#         'charge_point_id': charge_point_id,
#         'latitude': latitude,
#         'longitude': longitude,
#         'address': address,
#         'type': charge_point_type
#     }
#
#
# def getDate(date, time):
#     dateString = date + time
#     return datetime.strptime(dateString, "%Y%m%d%H%M")
