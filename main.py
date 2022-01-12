from math import floor
import pandas as pd
from requests.api import get
from geocodio import GeocodioClient
import requests
import json
import gspread
from datetime import datetime

client = GeocodioClient("1ad3fb6ad602a2fb61961ba3f306d2a00aa00b6")

saved_addresses = { 
    "HQ" : [-90.2884189,38.6080268] 
}

route = {
    "vehicles" : [],
    "jobs" : [],
}

jobs_df = pd.DataFrame()
vehicles_df = pd.DataFrame()

def geocode_addresses():
    global jobs_df, vehicles_df
    job_locations = client.batch_geocode(jobs_df.Address.tolist())

    print("Jobs Longitude")
    for i, row in jobs_df.iterrows():
        coords = [job_locations[i]['results'][0]['location']['lng'], job_locations[i]['results'][0]['location']['lat']]
        jobs_df.loc[i,"Latitude"] = coords[0]
        jobs_df.loc[i,"Latitude"] = coords[1]
        print(coords[0])
        
    print("Jobs Latitude")
    for location in job_locations:
        print(location['results'][0]['location']['lat'])


    vehicle_starts = client.batch_geocode(vehicles_df.Start.to_list())
    vehicle_ends = client.batch_geocode(vehicles_df.End.to_list())
    print("Vehicles Start Coords")
    for i, row in vehicles_df.iterrows():
        start_coords = [vehicle_starts[i]['results'][0]['location']['lng'], vehicle_starts[i]['results'][0]['location']['lat']]
        end_coords = [vehicle_ends[i]['results'][0]['location']['lng'], vehicle_ends[i]['results'][0]['location']['lat']]
        vehicles_df.loc[i, "Start Coords"] = start_coords
        vehicles_df.loc[i, "End Coords"] = end_coords
    


def open_sheet():
    # scope =["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]

    # Make the dataframe from Google Sheets 
    # creds = ServiceAccountCredentials.from_json_keyfile_name("greenroute20211227-3c59f1eb8cf2.json", scope)
    # client = gspread.authorize(creds)
    gc = gspread.service_account(filename="./greenroute20211227-3c59f1eb8cf2.json")
    # id = "1nai5YjxXcfIvxV-C6ZD5vNDjElhlO4PMr0ABUUO_Zjo"
    # sheet = client.open_by_key(id)
    sheet = gc.open("fyf_sample")
    return sheet

def getTimeString(this_time):
    minu = round((this_time - floor(this_time)) * 60) 
    hours = floor(this_time)
    ret_time = str(hours) + " hr " + str(minu) + " min"
    if this_time < 1:
        ret_time = str(minu) + " min"
    return ret_time

def update_sheet(sheet, response):
    global vehicles_df, jobs_df

    response = eval(response)
    routes = response['routes']
    
    # route_map= {'Order ID': 0, 'Order Type': 1, 'Date': 2, 'Due date': 3, 'Driver Serial Number': 4, 'Driver': 5, 'Vehicle': 6, 'Vehicle Label': 7, 'Stop Number': 8, 'Location ID': 9, 'Location': 10, 'Address': 11, 'Customer ID': 12, 'Customer': 13, 'Email': 14, 'Phone': 15, 'Route date': 16, 'Scheduled at': 17, 'Duration': 18, 'Time Window': 19, 'Boxes': 20, 'Notes': 21, 'Latitude': 22, 'Longitude': 23, 'Arrival Time': 24, 'Travel Time': 25, 'Travel Time [sec]': 26, 'Distance [mi]': 27}
    # for index, route in enumerate(routes_list[0]):
    #     route_map[route] = ""
    # print(route_map)

    i = 0
    output_df = pd.DataFrame({'Order ID': [], 'Order Type': [], 'Date': [], 'Due date': [], 'Driver Serial Number': [], 'Driver': [], 'Vehicle': [], 'Vehicle Label': [], 'Stop Number': [], 'Location ID': [], 'Location': [], 'Address': [], 'Customer ID': [], 'Customer': [], 'Email': [], 'Phone': [], 'Route date': [], 'Scheduled at': [], 'Duration': [], 'Time Window': [], 'Boxes': [], 'Notes': [], 'Latitude': [], 'Longitude': [], 'Arrival Time': [], 'Travel Time': [], 'Travel Time [sec]': [], 'Distance [m]': []})
    for route in routes:
        vehicle = route['vehicle']
        this_vehicle = vehicles_df[vehicles_df['ID'] == vehicle]
        # print(this_vehicle['Name'].iloc[0])
        for step in route['steps']:
            # print(step)
            if step['type'] == 'job':   
                output_df.loc[i,'Vehicle Label'] = vehicle 
                output_df.loc[i, 'Driver'] = this_vehicle['Name'].iloc[0]

                output_df.loc[i, 'Stop Number'] = step['id']
                output_df.loc[i, 'Longitude'] = step['location'][0]
                output_df.loc[i, 'Latitude'] = step['location'][1]

                duration = step['duration']/3600
                d_string = getTimeString(duration)
                
                arrival = step['arrival']/3600
                a_string = getTimeString(arrival)

                output_df.loc[i, 'Duration'] = d_string
                output_df.loc[i, 'Scheduled at'] = a_string
                output_df.loc[i, 'Distance [m]'] = step['distance']

                this_job = jobs_df[jobs_df['Latitude']==step['location'][1]].iloc[0]
                output_df.loc[i, 'Customer'] = this_job['Location Name']
                output_df.loc[i, 'Location ID'] = this_job['Location ID']
                output_df.loc[i, 'Order ID'] = this_job['Order ID']
                output_df.loc[i, 'Address'] = this_job['Address']

                # print(this_job['Location Name'].iloc[0])
                # print(step['duration']/3600)
                i+=1
    
    
    output_df.fillna("", inplace=True)
    sheet.update('A2:AZ1000', output_df.values.tolist())
        


def makeTimeWindowsObject(tw_from_list, tw_to_list):
    tw_objects = []
    for i in range(len(tw_from_list)):
            tw_from = datetime.strptime(tw_from_list[i].strip(), '%H:%M')
            tw_from = tw_from.hour*3600+tw_from.minute*60
            tw_to = datetime.strptime(tw_to_list[i].strip(), '%H:%M')
            tw_to = tw_to.hour*3600 + tw_to.minute*60
            tw_objects.append([tw_from, tw_to])
    return tw_objects

def main():
    global jobs_df, vehicles_df

    sheet = open_sheet()
    # df = pd.read_csv("fyf_sample_input.csv")
    jobs = sheet.worksheet('input_jobs')
    jobs_records = jobs.get_all_records()
    jobs_df = pd.DataFrame(jobs_records)

    # # Turn this on when you need to GeoCode the addresses
    # geocode_addresses()

    job_id = 0
    for i, row in jobs_df.iterrows():
        job = {
            "id": job_id,
            "location": [row['Longitude'],row['Latitude']]
        }
        route['jobs'].append(job)
        job_id += 1
    
    vehicles = sheet.worksheet('input_vehicles')
    vehicles_records = vehicles.get_all_records()
    vehicles_df = pd.DataFrame(vehicles_records)
    
    for i, row in vehicles_df.iterrows():
        start = row['Start Coords'].strip('][').split(',')
        start = [float(coord) for coord in start]
        end = row['End Coords'].strip('][').split(',')
        end = [float(coord) for coord in end]
        tw_from_list = row['TW from'].split(',')
        tw_to_list = row['TW to'].split(',')
        tw_objects = makeTimeWindowsObject(tw_from_list, tw_to_list)
        max_tasks = row['Max Tasks']
        
        vehicle = {
            "id" : len(route["vehicles"]),
            "start" : start,
            "end" : end,
            "time_windows" : tw_objects,
            "max_tasks" : max_tasks,
        }

        route["vehicles"].append(vehicle)
    
    # print(route)


    # Call the VROOM Demo Server
    url = "http://solver.vroom-project.org"
    header = {'content-type': 'application/json'}
    r = requests.post(url, data=json.dumps(route), headers=header)

    update_sheet(sheet.worksheet('program_output'), r.text)


if __name__ == '__main__':
    main()