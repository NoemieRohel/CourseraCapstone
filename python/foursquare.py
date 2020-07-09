import argparse
import requests
import datetime
import time
import pandas as pd


def read_credentials(file_path, userless=True):
    # Read the credentials from the file
    credentials = {}
    with open(file_path, 'r') as f:
        for line in f.readlines():
            key = line.split('=')[0]
            value = line.split('=')[1].replace('\n', '')
            credentials[key] = value

    if userless:
        result = {'client_id': credentials['client_id'], 'client_secret': credentials['client_secret']}
    else:
        result = {'oauth_token': credentials['oauth_token']}

    return result


def search_venues(fsa, credentials):
    # Get all the results from a foursquare search call
    # Define the url and parameters for the call
    url = 'https://api.foursquare.com/v2/venues/search'
    params = {
        'v': '20200608',
        'near': fsa,
        'limit': '1000'
    }
    params.update(credentials)

    response = requests.get(url, params)

    # Extract information from the response
    location_keys = ['address', 'lat', 'lng', 'cc', 'city', 'state', 'country']
    categories_keys = ['id', 'name', 'primary']
    venues = []

    for v in response.json()['response']['venues']:

        venue = {'FSA': fsa, 'id': v['id'], 'name': v['name']}

        for k in location_keys:
            if k in v['location'].keys():
                venue['location_' + k] = v['location'][k]
            else:
                venue['location_' + k] = None
                continue

        for k in categories_keys:
            if len(v['categories']) > 0 and k in v['categories'][0].keys():
                venue['category_' + k] = v['categories'][0][k]
            else:
                venue['category_' + k] = None
                continue

        venues.append(venue)

    return venues


def main():
    # Define default values
    cred_path = '../credentials.txt'
    output_folder = '../data/foursquare'
    output_file = 'foursquare'

    # Read the FSA from the CSV file
    df_fsa = pd.read_csv('../data/fsa.csv')
    fsas = df_fsa['FSA'].to_list()

    # Create the parser
    parser = argparse.ArgumentParser()

    # Create the scrap parser
    parser.add_argument('--fsa', required=True, choices=['all'] + fsas,
                        help='Name of the FSA where to search the venues or "all" to get the information for all FSA')
    parser.add_argument('--credentials_path', default=cred_path, help='A path to the credentials file')
    parser.add_argument('--output_folder', default=output_folder, help='The path of the output folder')
    parser.add_argument('--output_file', default=output_file, help='The name of the output file')

    args = parser.parse_args()

    # Get the information from the Foursquare API
    credentials = read_credentials(args.credentials_path)

    venues = []
    if args.fsa != 'all':
        venues = search_venues(args.fsa, credentials)
    else:
        nb_fsas = len(fsas)
        start_time = time.time()
        print('--- {} FSAs will be searched from Foursquare ---'.format(nb_fsas))
        for i in range(nb_fsas):
            fsa = fsas[i]
            venues += search_venues(fsa, credentials)
            print('--- Progression : {0:4.1f}% ({1}/{2} FSA(s) searched) ---'.format(i / nb_fsas * 100, i, nb_fsas))

        m, s = divmod(time.time() - start_time, 60)
        print('--- Progression : 100% ---')
        print('# total : {}'.format(nb_fsas))
        print('total process time : {0:.0f} min {1:.0f}'.format(m, s))

    # Export the information into a CSV file
    start_timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M')
    output_file_path = '{}/{}_{}.csv'.format(args.output_folder, args.output_file, start_timestamp)
    pd.DataFrame(venues).to_csv(output_file_path, index=False)
    print('The file {} has been created'.format(output_file_path))


if __name__ == '__main__':
    main()
