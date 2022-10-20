import time
from dbConnector import DbConnector
from readFiles import open_all_files


def clear_db(db):
    """
    Clears all data from the database
    """

    print(f"\n{time.strftime('%H:%M:%S')} Clearing existing trackpoints from database...")
    db.TrackPoint.delete_many({})

    print(f"\n{time.strftime('%H:%M:%S')} Clearing existing activities from database...")
    db.Activity.delete_many({})

    print(f"\n{time.strftime('%H:%M:%S')} Clearing existing users from database...\n")
    db.User.delete_many({})


def insert_data():
    """
    Insert data into the database
    """
    
    connection = DbConnector()
    db = connection.db

    # Starts with clearing the database
    clear_db(db)

    # Opens all files and returns a dictionary with all the data
    users_list, activities_list, trackpoints_list = open_all_files()

    # insert data into database
    print(f"\n{time.strftime('%H:%M:%S')} inserting {len(users_list)} users...")
    db.User.insert_many(users_list)

    print(f"\n{time.strftime('%H:%M:%S')} inserted {len(users_list)} users")

    print(f"\n{time.strftime('%H:%M:%S')} inserting {len(activities_list)} activities...")
    db.Activity.insert_many(activities_list)
    print(f"\n{time.strftime('%H:%M:%S')} inserted {len(activities_list)} activities")

    # Iterates through all trackpoints and inserts them into the database in batches of 1000
    counter = 0
    increment = 1000
    for i in range(0, int(len(trackpoints_list) / increment)):
        counter += increment
        print("{} Inserting trackpoints {:7.2f} % {:9,} / {:9,}".format(
            time.strftime("%H:%M:%S"),
            round(i / (len(trackpoints_list) / increment) * 100, 2),
            counter,
            len(trackpoints_list)
        ))
        
        db.TrackPoint.insert_many(trackpoints_list[counter:counter + increment])

    # We need to insert the rest of the trackpoints if the number of trackpoints is not divisible by 1000
    print("{} Inserting trackpoints {:7.2f} % {:9,} / {:9,}".format(
        time.strftime("%H:%M:%S"),
        100.00,
        len(trackpoints_list),
        len(trackpoints_list)
    )
    )
    db.TrackPoint.insert_many(trackpoints_list[:len(trackpoints_list) % increment])

    # Close database connection after all data is inserted
    connection.close_connection()

    print("\nInserted {:,} trackpoints".format(len(trackpoints_list)))