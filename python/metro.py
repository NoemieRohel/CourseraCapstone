import argparse
import time
import pandas as pd
import json
from geopy.geocoders import Nominatim

def main():
    # Define default values
    input_file = '../data/metro/metro_stations.geojson'
    output_folder = '../data/metro'
    output_file = 'metro_stations'

    # Create the parser
    parser = argparse.ArgumentParser()

    # Create the scrap parser
    parser.add_argument('--input_file', default=input_file, help='Geojson file with path for the metro stations')
    parser.add_argument('--output_folder', default=output_folder, help='The path of the output folder')
    parser.add_argument('--output_file', default=output_file, help='The name of the output file')

    args = parser.parse_args()

    # Get the info from the geojson file
    start_time = time.time()
    with open(args.input_file) as f:
        data = json.load(f)
    metro = []
    for feature in data['features']:
        if feature['geometry'] is not None:
            station = {}
            station['Name'] = feature['properties']['stop_name']
            station['Latitude'] = feature['geometry']['coordinates'][1]
            station['Longitude'] = feature['geometry']['coordinates'][0]
            metro.append(station)

    # Get the addresses from the coordinates to have the fsa
    print('---Getting the FSA for each station---')
    geolocator = Nominatim(user_agent='mtl')
    for m in metro:
        print('---Getting the FSA for {}---'.format(m['Name']))
        location = geolocator.reverse(str(m['Latitude']) + ', ' + str(m['Longitude']))
        m['fsa'] = location.address.split(',')[-2][1:4]

    m, s = divmod(time.time() - start_time, 60)
    print('# total : {}'.format(len(metro)))
    print('Total process time : {0:.0f} min {1:.0f}'.format(m, s))

    # Save the data into a csv file
    output_file_path = '{}/{}.csv'.format(args.output_folder, args.output_file)
    pd.DataFrame(metro).to_csv(output_file_path)


if __name__ == '__main__':
    main()