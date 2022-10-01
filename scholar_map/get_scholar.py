import argparse
import os
import logging
import jsonpickle
import tqdm
from scholarly import scholarly
from util import clean_filename

__author__ = 'Pedro Sequeira'
__email__ = 'pedrodbs@gmail.com'

AUTHOR_FILE = 'author.json'
AUTHORS_FILE = 'authors.json'

MIN_WAIT = 5
MAX_WAIT = 20
OUTPUT_DIR = 'output'

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--id', type=str, required=True,
                        help='The Google Scholar profile/user ID, i.e., what appears after '
                             'https://scholar.google.com/citations?user=')
    parser.add_argument('-o', '--output', type=str, default=OUTPUT_DIR,
                        help='The path to the directory in which to save data.')
    args = parser.parse_args()

    # output
    os.makedirs(args.output, exist_ok=True)
    logging.RootLogger.root.handlers = []
    handlers = [logging.FileHandler(os.path.join(args.output, '../scholar.log'), 'w', encoding='utf-8'),
                logging.StreamHandler()]
    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S',
                        handlers=handlers)

    # get author data
    author_file = os.path.join(args.output, AUTHOR_FILE)
    if os.path.isfile(author_file):
        with open(author_file, 'r') as fp:
            author = jsonpickle.loads(fp.read())
        logging.info(f'Loaded info for author id: "{args.id}" from {author_file}')
    else:
        logging.info(f'Getting info for author id: "{args.id}"...')
        search_query = scholarly.search_args.id(args.id)
        author = scholarly.fill(search_query)
        with open(author_file, 'w') as fp:
            fp.write(jsonpickle.dumps(author, indent=4))
        logging.info(f'Saved author info to {author_file}')

    # get citing authors data
    authors = {}
    authors_file = os.path.join(args.output, AUTHORS_FILE)
    if os.path.isfile(authors_file):
        with open(authors_file, 'r') as fp:
            authors = jsonpickle.loads(fp.read())
        logging.info(f'Loaded info for {len(authors)} citing authors from {authors_file}')

    # for each publication, get citations
    pubs = author['publications']
    logging.info(f'Got {len(pubs)} publications')
    total_cites = 0
    for pub in tqdm.tqdm(pubs):
        pub_id = pub['author_pub_id']
        pub_title = pub['bib']['title']

        # get citation data for this pub
        citations_file = os.path.join(args.output, clean_filename(pub_id) + '.json')
        if os.path.isfile(citations_file):
            with open(citations_file, 'r') as fp:
                citations = jsonpickle.loads(fp.read())
            logging.info(f'Loaded citations for "{pub_title}" from {citations_file}')
        else:
            logging.info(f'Getting citations for "{pub_title}"...')
            if 'citedby_url' not in pub:
                logging.info(f'"{pub_title}" does not have citations, skipping')
                continue
            citations = list(scholarly.citedby(pub))
            with open(citations_file, 'w') as fp:
                fp.write(jsonpickle.dumps(citations, indent=4))
            logging.info(f'Saved citations info to {citations_file}')
        total_cites += len(citations)

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

            # for each author, get info
            for i, args.id in enumerate(authors_ids):
                if i >= len(author_names):
                    continue
                name = author_names[i]
                if args.id in authors:
                    logging.info(f'Author "{name}" previously fetched')
                else:
                    # check no Google scholar for author, just store name
                    if args.id == '':
                        authors[name] = {'name': name}
                        logging.info(f'Author "{name}" does not have a Google Scholar profile')
                    else:
                        logging.info(f'Getting info for citing author "{name}"...')
                        search_query = scholarly.search_args.id(args.id)
                        authors[args.id] = search_query

                    # update authors file
                    with open(authors_file, 'w') as fp:
                        fp.write(jsonpickle.dumps(authors, indent=4))
                    logging.info(f'Updated authors info file: {authors_file}')

    logging.info('Done processing scholar')
    logging.info(f'Got {len(pubs)} articles, {total_cites} citations and {len(authors)} unique citing authors')
    logging.info(f'Saved results to {args.output}...')
    logging.info('Done!')
