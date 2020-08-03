import argparse
import requests
import time
import pandas as pd
import numpy as np
import json
import os


def read_credentials(file_path):
    # Read the credentials from the file
    credentials = {}
    with open(file_path, 'r') as f:
        for line in f.readlines():
            key = line.split('=')[0]
            value = line.split('=')[1].replace('\n', '')
            credentials[key] = value

    result = {'wsapikey': credentials['wsapikey']}

    return result


def get_fsa_points(fsa):
    # Get the coordinates for the generated points
    folder_path = '../data/walkscore/FSApoints'
    read_data = []
    for (dirpath, dirnames, filenames) in os.walk(folder_path):
        if fsa is not None:
            filenames = ['{}.geojson'.format(fsa)]
        for file_name in filenames:
            with open(os.path.join(dirpath, file_name)) as f:
                read_data.append(json.load(f))
    data = []
    for f in read_data:
        fsa = f['name']
        for feature in f['features']:
            if feature['geometry'] is not None:
                point = {}
                point['fsa'] = fsa
                point['latitude'] = feature['geometry']['coordinates'][1]
                point['longitude'] = feature['geometry']['coordinates'][0]
                data.append(point)

    return data


def get_walkscore(address, latitude, longitude, credentials):
    # Get the walkscore from an address, latitude and longitude
    # Define the url and parameters for the call
    url = 'https://api.walkscore.com/score'
    params = {
        'format': 'json',
        'address': address,
        'lat': latitude,
        'lon': longitude,
        'transit': '1',
        'bike': '1'
    }
    params.update(credentials)

    response = requests.get(url, params)

    # Extract information from the response
    walkscore = {}
    if 'walkscore' in response.json().keys():
        walkscore['Walkscore'] = response.json()['walkscore']
        walkscore['WalkscoreDescription'] = response.json()['description']
    if 'transit' in response.json().keys():
        walkscore['Transitscore'] = response.json()['transit']['score']
        walkscore['TransitDescription'] = response.json()['transit']['description']
    if 'bike' in response.json().keys():
        walkscore['Bikescore'] = response.json()['bike']['score']
        walkscore['BikeDescription'] = response.json()['bike']['description']

    # Get the error if any
    status_description = {'1': 'Walkscore successfully returned',
                          '2': 'Score is being calculated and is not currently available',
                          '30': 'Invalid latitude/longitude',
                          '31': 'Walkscore API internal error',
                          '40': 'WSAPIKEY is invalid',
                          '41': 'Daliy API quota has been exceeded',
                          '42': 'IP address has been blocked'}
    if walkscore == {}:
        status = str(response.json()['status'])
        print('ERROR Status {}: {}'.format(status, status_description[status]))

    return walkscore


def main():
    # Define default values
    cred_path = '../credentials.txt'
    output_folder = '../data/walkscore'
    output_file = 'walkscore'

    # Create the parser
    parser = argparse.ArgumentParser()

    # Create the scrap parser
    parser.add_argument('--address', default=None,
                        help='A specific address to get the Walkscore (latitude and longitude are needed)')
    parser.add_argument('--latitude', default=None, help='The latitude of the specific address')
    parser.add_argument('--longitude', default=None, help='The longitude of the specific address')
    parser.add_argument('--fsa', default=None,
                        help='A specific fsa to get the walkscore')
    parser.add_argument('--input_file', default=None, help='Input CSV file with path if available')
    parser.add_argument('--call_max', default=5000, help='The number max of remained calls to the API for the day')
    parser.add_argument('--credentials_path', default=cred_path, help='A path to the credentials file')
    parser.add_argument('--output_folder', default=output_folder, help='The path of the output folder')
    parser.add_argument('--output_file', default=output_file, help='The name of the output file')

    args = parser.parse_args()

    # Get the credentials for the Walkscore API
    credentials = read_credentials(args.credentials_path)

    output_file_path = '{}/{}.csv'.format(args.output_folder, args.output_file)
    if args.address is not None:
        # A specific address is given
        if args.latitude is None:
            print('The latitude of the address has to be given in arguments as well as the longitude')
        elif args.longitude is None:
            print('The longitude of the address has to be given in arguments as well as the latitude')
        else:
            start_time = time.time()
            walkscore = get_walkscore(args.address, args.latitude, args.longitude, credentials)
            walkscore['Address'] = args.address
            walkscore['Latitude'] = args.latitude
            walkscore['Longitude'] = args.longitude
            m, s = divmod(time.time() - start_time, 60)
            print('Total process time : {0:.0f} min {1:.0f}'.format(m, s))

            # Export the information into a CSV file
            pd.DataFrame([walkscore]).to_csv(output_file_path, index=False)
            print('The file {} has been created'.format(output_file_path))
    else:
        # All points of a FSA or all FSA
        if args.input_file is None:
            points = pd.DataFrame(get_fsa_points(args.fsa))
            ws_columns = ['Walkscore', 'WalkscoreDescription', 'Transitscore', 'TransitDescription',
                          'Bikescore', 'BikeDescription']
            points = points.reindex(columns=np.append(points.columns.values, ws_columns))
        else:
            points = pd.read_csv(args.input_file, index_col=0)
            output_file_path = args.input_file
        nb_call = 0
        start_time = time.time()
        for i in points.index:
            if np.isnan(points.loc[i, 'Walkscore']) and nb_call < int(args.call_max):
                print('---Getting the Walkscore for the point {}---'.format(i))
                walkscore = get_walkscore(points.loc[i, 'fsa'], points.loc[i, 'latitude'],
                                          points.loc[i, 'longitude'], credentials)
                for key in walkscore.keys():
                    points.loc[i, key] = walkscore[key]
                nb_call += 1
        m, s = divmod(time.time() - start_time, 60)
        print('# total : {}'.format(nb_call))
        print('Total process time : {0:.0f} min {1:.0f}'.format(m, s))

        # Export the data into a CSV file
        points.to_csv(output_file_path)
        print('The file {} has been created'.format(output_file_path))


if __name__ == '__main__':
    main()
