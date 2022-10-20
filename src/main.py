import argparse
import time
from datetime import datetime
from insertData import insert_data
from repository import Repository
import os
from tabulate import tabulate
    

def init_db():
    """
    Initialize the database
    """
    # Format the current time
    FMT = '%H:%M:%S'
    start_datetime = time.strftime(FMT)
    insert_data()
    end_datetime = time.strftime(FMT)
    # Calculate the time difference
    total_datetime = datetime.strptime(end_datetime, FMT) - datetime.strptime(start_datetime, FMT)
    print(f"Started: {start_datetime}\nFinished: {end_datetime}\nTotal: {total_datetime}")

def dataset_is_present() -> bool:
    if os.path.exists("../dataset"):
        return True
    else:
        return False


def main(should_init_db=False):

    if should_init_db:
        # Testing if dataset is in the correct folder
        if dataset_is_present():
            init_db()
        else:
            print("Dataset not found. Add 'dataset' to the root of the project folder")
            return

    query = Repository()

    table_format = 'github' # Table format for tabulate

    print("\n-------- Query 1 ----------")
    value = query.sum_user_activity_trackpoint()
    print(value)

    print("\n-------- Query 2 ----------")
    value = query.average_number_of_activities_per_user()
    print(value)

    print("\n-------- Query 3 ----------\n")
    headers = ['nr.','user id', 'activities']
    print(tabulate(query.top_twenty_users(), headers=headers, tablefmt=table_format))

    print("\n-------- Query 4 ----------")
    headers = ['user id']
    print("Users who have taken a taxi\n") 
    print(tabulate(query.users_taken_taxi(), headers=headers, tablefmt=table_format))

    print("\n-------- Query 5 ----------\n")
    headers = ['mode', 'count']
    print(tabulate(query.activity_transport_mode_count(), headers=headers, tablefmt=table_format))

    print("\n-------- Query 6 ----------")
    headers = ['year', 'hours']
    print(tabulate(query.year_with_most_activities(), headers=headers, tablefmt=table_format))

    print("\n-------- Query 7 ----------")
    value = query.total_distance_in_km_walked_in_2008_by_userid_112()
    print("The total distance walked in 2008 by user 112 is {:.2f} km".format(value))

    print("\n-------- Query 8 ----------")
    print("This one takes a while...\n")
    headers = ['nr.', 'user id', 'altitude']
    print(tabulate(query.top_20_users_gained_most_altitude_meters(), headers=headers, tablefmt=table_format))

    print("\n-------- Query 9 ----------")
    print("This one takes a while...\n")
    headers = ['user_id', 'invalid_activities']
    print(tabulate(query.invalid_activities_per_user(), headers=headers, tablefmt=table_format))

    print("\n-------- Query 10 ----------")
    values = query.users_tracked_activity_in_the_forbidden_city_beijing()
    print(''.join(values))

    print("\n-------- Query 11 ----------\n")
    headers = ['user id', 'transportation mode', 'count']
    print(tabulate(query.most_used_transportation_mode_per_user(), headers=headers, tablefmt=table_format))

    # Close the connection after all queries are executed
    query.connection.close_connection()


if __name__ == "__main__":
    # Enables flag to initialize database
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--init_database", action="store_true", help="Initialize the database")
    args = parser.parse_args()

    main(args.init_database)