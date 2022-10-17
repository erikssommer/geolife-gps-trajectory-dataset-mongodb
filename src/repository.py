from dis import dis
from turtle import distance
from dbConnector import DbConnector
from haversine import haversine
from pprint import pprint


class Repository:
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def sum_user_activity_trackpoint(self):
        """
        Query 1 - Finding how many users, activities and trackpoints are there in the dataset
        """

        user_sum = self.db.User.count()
        activity_sum = self.db.Activity.count()
        trackpoint_sum = self.db.TrackPoint.count()
        print("There are {} users, {:,} activities and {:,} trackpoints in the dataset".format(
            user_sum, activity_sum, trackpoint_sum).replace(",", " "))

    def average_number_of_activities_per_user(self):
        """
        Query 2 - Find the average number of activities per user.
        """
        res = self.db.Activity.aggregate([
            {
                '$group': {
                    '_id': '$user_id',
                    'sum': {
                        '$count': {}
                    }
                }
            }, {
                '$group': {
                    '_id': 'null',
                    'avg': {
                        '$avg': '$sum'
                    }
                }
            }
        ])

        print('The average number of activities per user is {:.2f}'.format(list(res)[0]['avg']))

    def top_twenty_users(self):
        """
        Query 3 - Find the top 20 users with the highest number of activities.
        """

        res = self.db.Activity.aggregate([
            {
                '$group': {
                    '_id': '$user_id',
                    'count': {
                        '$sum': 1
                    }
                }
            },
            {
                '$sort': {
                    'count': -1
                }
            },
            {
                '$limit': 20
            }
        ])

        print("nr. user_id activities\n")
        for i, user in enumerate(res):
            print("{:2} {:>8} {:>10}".format(i + 1, user['_id'], user['count']))

    def users_taken_taxi(self):
        """
        Query 4 - Find all users who have taken a taxi.
        """

        res = None

        print("Users who have taken a taxi: " + ", ".join([x[0] for x in res]))

    def activity_transport_mode_count(self):
        """
        Query 5 - Find all types of transportation modes and count how many activities 
        that are tagged with these transportation mode labels. 
        Does not count the rows where the mode is null.
        """

        res = None

        print("mode        count\n")
        for row in res:
            print("{:11} {:>5}".format(row[0], row[1]))

    def year_with_most_activities(self):
        """
        Query 6 - Find the year with the most activities. Testing if this also is the year with most recorded hours
        """
        # Query a - Find the year with the most activities.
        res6a = None
        print("The year {} has the most activities with {:,} activities".format(
            res6a[0][0], res6a[0][1]).replace(",", " "))

        # Query b - Testing if this also is the year with most recorded hours
        res6b = None
        print("The year {} has the most recorded hours with {:,} hours".format(
            res6b[0][0], res6b[0][1]).replace(",", " "))

        print("\nyear   hours\n")
        for row in res6b:
            print("{}  {:>6,}".format(row[0], row[1]).replace(",", " "))

        # Testing if the year with most activities also is the year with most recorded hours
        if res6b[0][0] == res6a[0][0]:
            print("\nYes, this is also the year with most recorded hours!")
        else:
            print("\nNo, this is not the year with most recorded hours")

    def total_distance_in_km_walked_in_2008_by_userid_112(self):
        """ 
        Query 7 - Find the total distance (in km) walked in 2008, by user with id = 112
        """
        # Finding longitude and latitude for each trackpoint matching the user_id, mode and year
        res = None

        distance = 0
        for i in range(len(res)):
            if i == len(res)-1:
                break
            # Calculating distance between two points using haversine formula
            distance += haversine(res[i], res[i+1])

        print("The total distance walked in 2008 by user 112 is {:.2f} km".format(distance))

    def top_20_users_gained_most_altitude_meters(self):
        """
        Query 8 - Find the top 20 users who have gained the most altitude meters.
        """

        res = None

        trackpoint_altitudes = res
        user_altitude = dict()

        # Calculating the altitude gained for each user
        for index in range(len(trackpoint_altitudes)):
            # Breaking if the last trackpoint is reached
            if index == len(trackpoint_altitudes) - 1:
                break

            user_id = trackpoint_altitudes[index][0]
            activity_id = trackpoint_altitudes[index][1]

            next_activity_id = trackpoint_altitudes[index + 1][1]

            # We can only calculate the altitude gain if we have two trackpoints from the same activity
            if activity_id != next_activity_id:
                continue

            # Initialize the user_altitude dict if the user_id is not in it
            if user_id not in user_altitude:
                user_altitude[user_id] = 0

            altitude = trackpoint_altitudes[index][2]
            next_altitude = trackpoint_altitudes[index + 1][2]

            # If one of the altitudes are null they were -777 before cleanup and are invalid
            if not altitude or not next_altitude:
                continue

            altitude_diff = next_altitude - altitude
            user_altitude[user_id] += altitude_diff

        # Sorting the dict by the altitude gained
        user_altitude_array = sorted(
            user_altitude.items(), key=lambda x: x[1], reverse=True)

        print("nr. user_id altitude\n")
        for i, (user_id, altitude) in enumerate(user_altitude_array[:20]):
            print("{:3} {:>7} {:>8}".format(i + 1, user_id, altitude))

    def invalid_activities_per_user(self):
        """
        Query 9 - Find all users who have invalid activities, and the number of invalid activities per user
        """

        res = None

        print("user_id  invalid_activities\n")
        for row in res:
            print("{} {:>23}".format(row[0], row[1]))

    def users_tracked_activity_in_the_forbidden_city_beijing(self):
        """
        Query 10 - Find the users who have tracked an activity in the Forbidden City of Beijing.
        """

        query = None
        res = self.execute_query(query)
        for row in res:
            print("User {} has {} trackpoints in the forbidden city".format(
                row[0], row[1]))

    def users_registered_transportation_mode_and_their_most_used_transportation_mode(self):
        """
        Query 11 - Find all users who have registered transportation_mode and their most used transportation_mode
        """

        res = None

        if len(res) == 0:
            print("No users have registered transportation mode")

        print("user_id transportation_mode count\n")
        for user in res:
            print("{:7} {:18} {:6}".format(user[0], user[1], user[2]))
