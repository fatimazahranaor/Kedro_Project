from kedro.pipeline import node
import logging

logger = logging.getLogger(__name__) 

def limit_data_size(data ,limit_size):
    print("node Started !!!")
    if limit_size is None:
        logger.warning("No limit size provided !")
        return data
    return data.head(limit_size)


# def rename_columns(data ,old , new):
#     print("node Started !!!")
#     data.rename(columns={old : new})
#     return data

def rename_columns(data ):
    print("node Started !!!")
    # for columnName in data.columns:
    #     # print(columnName)
    #     print(columnName.capitalize())
    #     data.rename(columns={columnName : columnName.capitalize()})

    data.columns = [x.capitalize() for x in data.columns]

    return data

def clean_data(data, maxLat , maxLong):
    print("node Started !!!")

    data = data.drop(data[(data.pickup_latitude > maxLat) | (data.pickup_latitude < -maxLat) | (data.dropoff_latitude > maxLat) | (data.dropoff_latitude < -maxLat)].index)
    data = data.drop(data[(data.pickup_longitude > maxLong) | (data.pickup_longitude < -maxLong) | (data.pickup_longitude > maxLong) | (data.pickup_longitude < -maxLong)].index)


    return data