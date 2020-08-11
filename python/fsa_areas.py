import argparse
import pandas as pd
from selenium import webdriver

def main():
    # Define default values
    driver_path = r'C:\Users\noemi\Documents\chromedriver.exe'
    output_folder = '../data'
    output_file = 'fsa_areas'

    # Create the parser
    parser = argparse.ArgumentParser()

    # Create the scrap parser
    parser.add_argument('--driver_path--', default=driver_path, help='The path to the Chrome driver (with an r before)')
    parser.add_argument('--output_folder', default=output_folder, help='The path of the output folder')
    parser.add_argument('--output_file', default=output_file, help='The name of the output file')

    args = parser.parse_args()

    # Scrap the website with the info
    driver = webdriver.Chrome(driver_path)
    driver.get('https://postal-codes.cybo.com/canada/quebec/#listcodes')

    table = driver.find_element_by_xpath("//table[@style='border-collapse: collapse;']").text.split('\n')

    fsa_areas = []
    for line in table:
        if line[0:2] in ['H1', 'H2', 'H3', 'H4', 'H5', 'H8', 'H9'] and line[3] == ' ':
            data = {}
            data['FSA'] = line[0:3]
            if line.split(' ')[-1] == 'km²':
                data['Area(km2)'] = float(line.split(' ')[-2])
            elif line.split(' ')[-1] == 'm²':
                area = line.split(' ')[-2].replace(',', '')
                data['Area(km2)'] = float(area) / 1000000
            else:
                data['Area(km2)'] = None
            fsa_areas.append(data)

    driver.quit()

    # Save the data into a csv file
    output_file_path = '{}/{}.csv'.format(args.output_folder, args.output_file)
    pd.DataFrame(fsa_areas).to_csv(output_file_path)


if __name__ == '__main__':
    main()