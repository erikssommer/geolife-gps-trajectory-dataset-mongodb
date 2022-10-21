from dbConnector import DbConnector
from haversine import haversine


class Repository:
    """
    Class for querying the MongoDB database
    """

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
        return "There are {} users, {:,} activities and {:,} trackpoints in the dataset".format(
            user_sum, activity_sum, trackpoint_sum).replace(",", " ")

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

        return 'The average number of activities per user is {:.2f}'.format(list(res)[0]['avg'])

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

        result = []
        counter = 0
        for row in res:
            counter+=1
            result.append([counter, row["_id"], row["count"]])
        
        return result

    def users_taken_taxi(self):
        """
        Query 4 - Find all users who have taken a taxi.
        """

        res = self.db.Activity.aggregate([
            {
                '$match': {
                    'transportation_mode': 'taxi'
                }
            },
            {
                '$group': {
                    '_id': '$user_id'
                }
            },
            {
                '$sort': {
                    '_id': 1
                }
            }
        ])

        result = []
        for row in res:
            result.append([row['_id']])
        
        return result

    def activity_transport_mode_count(self):
        """
        Query 5 - Find all types of transportation modes and count how many activities 
        that are tagged with these transportation mode labels. 
        Does not count the rows where the mode is null.
        """

        res = self.db.Activity.aggregate([
            {
                '$match': {
                    'transportation_mode': {
                        '$ne': ''
                    }
                }
            },
            {
                '$group': {
                    '_id': '$transportation_mode',
                    'count': {
                        '$sum': 1
                    }
                }
            },
            {
                '$sort': {
                    'count': -1
                }
            }
        ])

        result = []
        for row in res:
            result.append([row['_id'], row['count']])

        return result


    def year_with_most_activities(self):
        """
        Query 6 - Find the year with the most activities. Testing if this also is the year with most recorded hours
        """
        # Query a - Find the year with the most activities.
        res6a = self.db.Activity.aggregate([
            {
                '$group': {
                    '_id': {
                        '$year': '$start_date_time'
                    },
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
                '$limit': 1
            }
        ])

        obj_res_6a = list(res6a)
        year_a = obj_res_6a[0]['_id']
        count_a = obj_res_6a[0]['count']

        print("The year {} has the most activities with {:,} activities".format(
            year_a, count_a).replace(",", " "))

        # Query b - Testing if this also is the year with most recorded hours
        res6b = self.db.Activity.aggregate([
            {
                '$group': {
                    '_id': {
                        '$year': '$start_date_time'
                    },
                    'sum': {
                        '$sum': {
                            '$divide': [
                                {
                                    '$subtract': [
                                        '$end_date_time', '$start_date_time'
                                    ]
                                }, 1000 * 60 * 60
                            ]
                        }
                    }
                }
            },
            {
                '$sort': {
                    'sum': -1
                }
            },
            {
                '$limit': 5
            }
        ])

        obj_res_6b = list(res6b)
        year_b = obj_res_6b[0]['_id']
        sum_b = obj_res_6b[0]['sum']

        print("The year {} has the most recorded hours with {:,} hours".format(
            year_b, round(sum_b)).replace(",", " "))

        # Testing if the year with most activities also is the year with most recorded hours
        if year_a == year_b:
            print("\nYes, this is also the year with most recorded hours!\n")
        else:
            print("\nNo, this is not the year with most recorded hours\n")

        result = []

        for row in obj_res_6b:
            result.append([row['_id'], round(row['sum'])])
        
        return result

    def total_distance_in_km_walked_in_2008_by_userid_112(self):
        """ 
        Query 7 - Find the total distance (in km) walked in 2008, by user with id = 112
        """

        # Get all activity ids for user 112 in 2008 with transportation mode walk
        res_activities = self.db.Activity.find({
            "user_id": "112",
            "transportation_mode": "walk"
        }, {
            "_id": False,
            "id": True
        })

        activities_list = [x['id'] for x in res_activities]

        # Use the activity ids to get the related trackpoints
        res_trackpoints = self.db.TrackPoint.find({
            "activity_id": {
                "$in": activities_list
            }
        }, {
            "_id": False,
            "id": True,
            "activity_id": True,
            "lat": True,
            "lon": True
        })

        trackpoints_list = list(res_trackpoints)

        distance = 0

        # Loop through the trackpoints and calculate the distance between each point
        for i in range(len(trackpoints_list) - 1):
            activity_id = trackpoints_list[i]['activity_id']
            next_activity_id = trackpoints_list[i + 1]['activity_id']

            # If the next trackpoint is not in the same activity we skip
            # as we only want to calculate the distance between trackpoints in the same activity
            if activity_id != next_activity_id:
                continue
            
            lat1 = trackpoints_list[i]['lat']
            lon1 = trackpoints_list[i]['lon']
            lat2 = trackpoints_list[i + 1]['lat']
            lon2 = trackpoints_list[i + 1]['lon']

            # Calculate the distance between the two points using haversine
            distance += haversine((lat1, lon1), (lat2, lon2))

        return distance

    def top_20_users_gained_most_altitude_meters(self):
        """
        Query 8 - Find the top 20 users who have gained the most altitude meters.
        """

        res = self.db.TrackPoint.find({}, {
            '_id': False,
            'user_id': True,
            'activity_id': True,
            'altitude': True  
        })

        trackpoint_altitudes = list(res)
        user_altitude = dict()

        # Calculating the altitude gained for each user
        for index in range(len(trackpoint_altitudes)):
            # Breaking if the last trackpoint is reached
            if index == len(trackpoint_altitudes) - 1:
                break

            user_id = trackpoint_altitudes[index]['user_id']
            activity_id = trackpoint_altitudes[index]['activity_id']
            next_activity_id = trackpoint_altitudes[index + 1]['activity_id']

            # We can only calculate the altitude gain if we have two trackpoints from the same activity
            if activity_id != next_activity_id:
                continue

            # Initialize the user_altitude dict if the user_id is not in it
            if user_id not in user_altitude:
                user_altitude[user_id] = 0

            altitude = trackpoint_altitudes[index]['altitude']
            next_altitude = trackpoint_altitudes[index + 1]['altitude']

            # If one of the altitudes are null they were -777 before cleanup and are invalid
            if not altitude or not next_altitude:
                continue

            altitude_diff = next_altitude - altitude
            user_altitude[user_id] += altitude_diff

        # Sorting the dict by the altitude gained
        user_altitude_array = sorted(
            user_altitude.items(), key=lambda x: x[1], reverse=True)

        result = []

        for i, (user_id, altitude) in enumerate(user_altitude_array[:20]):
            result.append([i + 1, user_id, round(altitude)])
        
        return result

    def invalid_activities_per_user(self):
        """
        Query 9 - Find all users who have invalid activities, and the number of invalid activities per user
        """

        res = self.db.TrackPoint.find({}, {
            '_id': False,
            'user_id': True,
            'activity_id': True,
            'date_time': True
        })

        trackpoints = list(res)
        invalid_user_activities = dict()

        # Comparing every two trackpoints to see if they are invalid
        for index in range(len(trackpoints)):
            # Breaking if the last trackpoint is reached
            if index == len(trackpoints) - 1:
                break

            user_id = trackpoints[index]['user_id']
            activity_id = trackpoints[index]['activity_id']
            next_activity_id = trackpoints[index + 1]['activity_id']

            # We can only compare if we have two trackpoints from the same activity
            if activity_id != next_activity_id:
                continue

            # Initialize the invalid_activities dict if the user_id is not in it
            if user_id not in invalid_user_activities:
                invalid_user_activities[user_id] = set()

            date_time = trackpoints[index]['date_time']
            next_date_time = trackpoints[index + 1]['date_time']

            date_time_diff = next_date_time - date_time
            if date_time_diff.seconds > 60 * 5:
                invalid_user_activities[user_id].add(activity_id)

        # Sorting the dict by the date_time gained
        sorted_invalid_activities = sorted(invalid_user_activities.items())

        result = []

        for user_id, activities in sorted_invalid_activities:
            result.append([user_id, len(activities)])
        
        return result

    def users_tracked_activity_in_the_forbidden_city_beijing(self):
        """
        Query 10 - Find the users who have tracked an activity in the Forbidden City of Beijing.
        """

        res = self.db.TrackPoint.aggregate([
            {
                '$match': {
                'lat': {
                    '$gte': 39.916,
                    '$lte': 39.917
                },
                'lon': {
                    '$gte': 116.397,
                    '$lte': 116.398
                }
                }
            },
            {
                '$group': {
                '_id': '$user_id',
                }
            }
        ])

        result = []

        for row in list(res):
            result.append(f"User {row['_id']} has trackpoints in the forbidden city\n")

        return result

    def most_used_transportation_mode_per_user(self):
        """
        Query 11 - Find all users who have registered transportation_mode and their most used transportation_mode
        """

        # Getting all users who have registered a transportation_mode
        res = self.db.Activity.find({
            'transportation_mode': {
                '$ne': ''
            }
        }, {
            '_id': False,
            'user_id': True,
            'transportation_mode': True
        })

        activities = list(res)

        user_transportation_mode = dict()

        for activity in activities:
            user_id = activity['user_id']
            transportation_mode = activity['transportation_mode']

            # Initialize the user_transportation_mode dict if the user_id is not in it
            if user_id not in user_transportation_mode:
                user_transportation_mode[user_id] = dict()

            # Initialize the transportation_mode dict if the transportation_mode is not in it
            if transportation_mode not in user_transportation_mode[user_id]:
                user_transportation_mode[user_id][transportation_mode] = 0

            # Increment the count for the given transportation_mode
            user_transportation_mode[user_id][transportation_mode] += 1

        # Sorting the dict by the date_time gained
        sorted_user_transportation_mode = sorted(user_transportation_mode.items())

        result = []

        for user_id, transportation_modes in sorted_user_transportation_mode:
            transportation_mode = max(transportation_modes, key=transportation_modes.get)
            result.append([user_id, transportation_mode, transportation_modes[transportation_mode]])
        
        return result
