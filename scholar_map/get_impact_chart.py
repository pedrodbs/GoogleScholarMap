import argparse
import logging
import os
import jsonpickle
import pandas as pd
import tqdm
from get_locations import LOCATIONS_FILE
from get_scholar import OUTPUT_DIR, AUTHOR_FILE, AUTHORS_FILE
from util import clean_filename, get_title

__author__ = 'Pedro Sequeira'
__email__ = 'pedrodbs@gmail.com'

PUB_COL_NAME = 'Original Contribution'
CITATION_COL_NAME = 'Cited By'
INSTITUTE_COL_NAME = 'Institute'
LOCATION_COL_NAME = 'Location'
DOMAIN_COL_NAME = 'Domain'

IMPACT_CHART_FILE = 'impact_chart.csv'


def _get_author_info():
    sub_domain = cite_author['email_domain'].lower().replace('@', '')
    info = domains_df[domains_df['domain'] == sub_domain]
    if len(info) > 0:
        return info.iloc[0]

    domain = '.'.join(sub_domain.split('.')[-2:])
    info = domains_df[domains_df['domain'] == domain]
    if len(info) > 0:
        return info.iloc[0]

    return None  # not found


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--id', type=str, required=True,
                        help='The Google Scholar profile/user ID, i.e., what appears after '
                             'https://scholar.google.com/citations?user=')
    parser.add_argument('-o', '--output', type=str, default=OUTPUT_DIR,
                        help='The path to the directory to load and save data.')
    args = parser.parse_args()

    # output
    os.makedirs(args.output, exist_ok=True)
    logging.RootLogger.root.handlers = []
    handlers = [logging.FileHandler(os.path.join(args.output, 'impact.log'), 'w', encoding='utf-8'),
                logging.StreamHandler()]
    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S',
                        handlers=handlers)

    # get author data
    file_path = os.path.join(args.output, AUTHOR_FILE)
    if not os.path.isfile(file_path):
        raise ValueError(f'File with author\'s info does not exist: {file_path}')
    with open(file_path, 'r') as fp:
        author = jsonpickle.loads(fp.read())
    logging.info(f'Loaded info for author id: "{args.id}" from {file_path}')

    # get citing authors data
    file_path = os.path.join(args.output, AUTHORS_FILE)
    if not os.path.isfile(file_path):
        raise ValueError(f'File with citing authors\' info does not exist: {file_path}')
    with open(file_path, 'r') as fp:
        authors = jsonpickle.loads(fp.read())
    logging.info(f'Loaded info for {len(authors)} citing authors from {file_path}')

    # get domain data
    file_path = os.path.join(args.output, LOCATIONS_FILE)
    if not os.path.isfile(file_path):
        raise ValueError(f'File with domain info does not exist: {file_path}')
    domains_df = pd.read_csv(file_path)
    logging.info(f'Loaded location data from "{file_path}"')

    # for each publication, get citations
    pubs = author['publications']
    logging.info(f'Got {len(pubs)} publications')
    total_cites = 0
    impact_data = []
    logging.info('==================================================')
    logging.info('Taking citations\' institute and location information for each publication...')
    for pub in tqdm.tqdm(pubs):
        pub_id = pub['author_pub_id']
        pub_title = pub['bib']['title']

        # get citation data for this pub
        citations_file = os.path.join(args.output, clean_filename(pub_id) + '.json')
        if not os.path.isfile(citations_file):
            logging.info(f'File with citations info for "{pub_title}" does not exist: {citations_file} (no citations?)')
            continue
        with open(citations_file, 'r') as fp:
            citations = jsonpickle.loads(fp.read())
        logging.info(f'Loaded citations for "{pub_title}" from {citations_file}')

        citation_data = {INSTITUTE_COL_NAME: [], LOCATION_COL_NAME: [], DOMAIN_COL_NAME: []}
        impact_data.append({PUB_COL_NAME: pub_title, CITATION_COL_NAME: citation_data})

        # for each citation, get authors' list
        logging.info(f'Processing {len(citations)} citations...')
        for citation in citations:
            authors_ids = citation['args.id']
            author_names = citation['bib']['author']

            # check own citation, skip
            if args.id in authors_ids:
                total_cites -= 1
                logging.info('Skipping own citation')
                continue

            # for each author, get country
            for i, args.id in enumerate(authors_ids):
                if i >= len(author_names):
                    continue
                name = author_names[i]
                if args.id == '' or args.id not in authors:
                    logging.info(f'Author "{name}" not found or does not have a Google Scholar profile')
                    continue
                cite_author = authors[args.id]

                author_info = _get_author_info()
                if author_info is None:
                    logging.info(f'Author "{args.id}"\'s country not found, skipping')
                affiliation = author_info['name']
                if affiliation not in citation_data[INSTITUTE_COL_NAME]:
                    citation_data[INSTITUTE_COL_NAME].append(affiliation)
                    citation_data[LOCATION_COL_NAME].append(author_info['country'])
                    citation_data[DOMAIN_COL_NAME].append(author_info['domain'])

    logging.info('==================================================')
    file_path = os.path.join(args.output, IMPACT_CHART_FILE)
    with open(file_path, 'w', encoding='utf-8') as fp:
        for pub in impact_data:
            if len(pub[CITATION_COL_NAME][INSTITUTE_COL_NAME]) == 0:
                continue  # no citations, skip
            fp.write(f'{PUB_COL_NAME},{CITATION_COL_NAME}, , \n')
            title = get_title(pub[PUB_COL_NAME])
            fp.write(f'"{title}",{INSTITUTE_COL_NAME},{LOCATION_COL_NAME},{DOMAIN_COL_NAME}\n')
            institutes = pub[CITATION_COL_NAME][INSTITUTE_COL_NAME]
            locations = pub[CITATION_COL_NAME][LOCATION_COL_NAME]
            domains = pub[CITATION_COL_NAME][DOMAIN_COL_NAME]
            df = pd.DataFrame(pub[CITATION_COL_NAME])
            df.sort_values([LOCATION_COL_NAME, INSTITUTE_COL_NAME], inplace=True)
            for _, row in df.iterrows():
                fp.write(f' ,"{row[INSTITUTE_COL_NAME]}","{row[LOCATION_COL_NAME]}","{row[DOMAIN_COL_NAME]}"\n')
            fp.write(f' , , ,\n')  # blank line
    logging.info(f'Saved impact chart in "{file_path}"')

    logging.info('Done!')
