import requests
import yaml
import json
import argparse
import time
import datetime
import pandas as pd
import numpy as np


def extract_infos_from_census(dguid, topic, topic_infos, lang):
    # Get the information from the census for a specific topic and a dguid
    # Call the API
    url = 'https://www12.statcan.gc.ca/rest/census-recensement/CPR2016.json'

    params = {
        'lang': lang,
        'dguid': dguid,
        'topic': topic_infos['topic_id'],
    }

    response = requests.get(url, params)
    result = json.loads(response.text[2:])
    res = pd.DataFrame(result['DATA'], columns=result['COLUMNS'])

    # Process the result
    df = pd.DataFrame()

    for cat in topic_infos['categories_single']:
        dfi = res.loc[res.HIER_ID == cat, ['TEXT_NAME_NOM', 'T_DATA_DONNEE', 'HIER_ID']].reset_index()
        dfi = dfi.rename(columns={'T_DATA_DONNEE': 'CategoryValue',
                                  'TEXT_NAME_NOM': 'Category',
                                  'HIER_ID': 'CategoryHierId'})
        df = df.append(dfi)

    if topic_infos['categories_plural'] == '':
        df['TypeValue'] = np.nan
        df['Type'] = np.nan
        df['TypeHierId'] = np.nan

    else:
        for cat in topic_infos['categories_plural']:
            typ = [i for i in res.HIER_ID if i.startswith(cat + '.')]

            if len(typ) == 0:
                dfi = res.loc[res.HIER_ID == cat, ['TEXT_NAME_NOM', 'T_DATA_DONNEE', 'HIER_ID']].reset_index()
                dfi = dfi.rename(columns={'T_DATA_DONNEE': 'CategoryValue',
                                        'TEXT_NAME_NOM': 'Category',
                                        'HIER_ID': 'CategoryHierId'})
            else:
                dfi = res.loc[res.HIER_ID.isin(typ), ['TEXT_NAME_NOM',
                                                      'T_DATA_DONNEE',
                                                      'HIER_ID']].reset_index(drop=True)
                dfi['Category'] = res.loc[res.HIER_ID == cat, 'TEXT_NAME_NOM'].values[0]
                dfi['CategoryValue'] = res.loc[res.HIER_ID == cat, 'T_DATA_DONNEE'].values[0]
                dfi['CategoryHierId'] = cat
                dfi = dfi.rename(columns={'T_DATA_DONNEE': 'TypeValue',
                                          'TEXT_NAME_NOM': 'Type',
                                          'HIER_ID': 'TypeHierId'})

            df = df.append(dfi)

    df['Topic'] = topic
    df['idTopic'] = topic_infos['topic_id']

    columns = ['idTopic', 'Topic', 'CategoryHierId', 'Category', 'TypeHierId', 'Type', 'TypeValue', 'CategoryValue']
    df = df[columns].reset_index(drop=True)

    return df


def main():
    # Define default values
    output_folder = '../data/census'
    output_file = 'census'
    dguid_template = "2016A0011{FSA}"

    # Read the config file
    config_file = '../data/census/config.yml'

    with open(config_file) as cf:
        config = yaml.load(cf, Loader=yaml.BaseLoader)

    all_topics = list(config.keys())

    # Read the fsa file
    df_fsa = pd.read_csv('../data/fsa.csv')
    all_fsas = df_fsa['FSA'].to_list()

    # Create the parser
    parser = argparse.ArgumentParser()

    parser.add_argument('--fsa', required=True, choices=['all'] + all_fsas, help='Name of the FSA of interest')
    parser.add_argument('--topic', required=True, choices=['all'] + all_topics, help='Name of the topic of interest')
    parser.add_argument('--lang', default='F', help='The path of the output folder')
    parser.add_argument('--output_folder', default=output_folder, help='The path of the output folder')
    parser.add_argument('--output_file', default=output_file, help='The name of the output file')

    args = parser.parse_args()

    # Retrieve all the information from census
    if args.fsa == 'all':
        fsas = all_fsas
    else:
        fsas = [args.fsa]

    if args.topic == 'all':
        topics = all_topics
    else:
        topics = [args.topic]

    df = pd.DataFrame()
    start_time = time.time()
    nb_fsas = len(fsas)

    print('--- {} FSAs will be searched from the Census ---'.format(nb_fsas))
    for i in range(nb_fsas):
        fsa = fsas[i]
        dguid = dguid_template.replace('{FSA}', fsa)

        for topic in topics:
            dfi = extract_infos_from_census(dguid, topic, config[topic], lang=args.lang)
            dfi['FSA'] = fsa
            df = df.append(dfi)

        print('--- Progression : {0:4.1f}% ({1}/{2} FSA(s) searched) ---'.format(i / nb_fsas * 100, i, nb_fsas))

    m, s = divmod(time.time() - start_time, 60)
    print('--- Progression : 100% ---')
    print('# total : {}'.format(nb_fsas))
    print('total process time : {0:.0f} min {1:.0f}'.format(m, s))

    # Export the information into a CSV file
    start_timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M')
    output_file_path = '{}/{}_{}.csv'.format(args.output_folder, args.output_file, start_timestamp)
    df.to_csv(output_file_path, index=False)
    print('The file {} has been created'.format(output_file_path))


if __name__ == "__main__":
    main()
