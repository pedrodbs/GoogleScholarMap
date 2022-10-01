import argparse
import csv
import logging
import os.path
import re
import email2country
import jsonpickle
import tqdm
import IP2Location
import socket
import pandas as pd
from ipaddress import ip_network, ip_address
from geopy import Nominatim
from geotext import GeoText
from requests.exceptions import SSLError
from get_scholar import AUTHORS_FILE, OUTPUT_DIR
from util import get_title

__author__ = 'Pedro Sequeira'
__email__ = 'pedrodbs@gmail.com'

LOCATIONS_FILE = 'locations.csv'

# DATA files
IP_LOCATION_DB = 'data/IP2LOCATION-LITE-DB9.BIN/IP2LOCATION-LITE-DB9.BIN'
WORLD_UNI_DOMAINS = 'data/world_universities_and_domains.json'
US_UNI_DATA = 'data/Colleges_and_Universities.csv'
ISP_NAMES_DB = 'data/IP2LOCATION-LITE-ASN.CSV'

US_CODE_TO_COUNTRY = {
    'US': 'United States',
    'PR': 'Puerto Rico',
    'VI': 'Virgin Islands, US',
    'FS': 'Federated States of Micronesia',
    'MP': 'Northern Mariana Islands',
    'AS': 'American Samoa',
    'GU': 'Guam',
    'MH': 'Marshall Islands',
    'PW': 'Palau'
}

EQUIV_COUNTRIES = {
    'korea (republic of)': 'south korea',
    'korea, republic of': 'south korea',
    'united kingdom of great britain and northern ireland': 'united kingdom',
    'ireland': 'united kingdom',
    'united states of america': 'united states'
}


def _search_us_unis(domain):
    unis = us_unis_df[us_unis_df['website'].str.endswith(domain)]
    if len(unis) == 0:
        return None
    uni = unis.iloc[0]
    if len(unis) > 1:
        logging.info(f'Got {len(unis)} universities matching "{domain}",\n{unis["name"]}\nSelecting best...')
        uni = unis.loc[unis['website'].map(lambda x: len(x.replace(domain, ''))).sort_values().index].iloc[0]
    uni = dict(uni[['name', 'address', 'city', 'state', 'zip', 'country', 'latitude', 'longitude']])
    uni['country'] = US_CODE_TO_COUNTRY[uni['country']]
    uni['domain'] = domain
    _register_domain(domain, uni, uni['name'])
    logging.info(f'Found info for domain "{domain}": {uni}')
    return uni


def _search_domain_ip(domain, affiliation, country):
    try:
        ip_addr = socket.gethostbyname(domain)
        ip_info = ip_database.get_all(ip_addr)
        ip_info.country_long = ip_info.country_long.lower()
        if country is not None and ip_info.country_long != country and \
                (ip_info.country_long not in EQUIV_COUNTRIES or EQUIV_COUNTRIES[ip_info.country_long] != country):
            logging.info(
                f'Found IP for domain "{domain}" but got inconsistent country: "{ip_info.country_long}"!="{country}"')
            return  # can't trust in IP info..
        if affiliation is None:
            if ip_info.isp is not None:
                affiliation = ip_info.isp
            else:
                ip_addr = ip_address(ip_addr)  # search for ISP name in database
                matches = isp_names_df[isp_names_df['ip'].map(lambda ip: ip_addr in ip_network(ip))]
                if len(matches) > 0:
                    affiliation = matches.iloc[0]['name']
        country = country if country is not None else EQUIV_COUNTRIES[ip_info.country_long] \
            if ip_info.country_long in EQUIV_COUNTRIES else ip_info.country_long
        uni = dict(domain=domain, name=affiliation, city=ip_info.city, state=ip_info.region, zip=ip_info.zipcode,
                   country=country, latitude=ip_info.latitude, longitude=ip_info.longitude)
        logging.info(f'Found info via IP search for domain "{domain}": {uni}')
        _register_domain(domain, uni, affiliation)
        return uni
    except (socket.gaierror, ValueError) as err:
        logging.info(f'Error: {err}')
    return None  # no luck


def _search_world_unis(domain):
    for uni in world_unis:
        if not any(domain in d for d in uni['domains']) and not any(domain in w for w in uni['web_pages']):
            continue

        # found uni, try searching US uni database for name as it has more info
        uni_name = uni['name'].lower()
        unis = us_unis_df[us_unis_df['name'].str.contains(uni_name) | us_unis_df['alias'].str.contains(uni_name)]
        if len(unis) >= 1:
            uni = unis.iloc[0]
            if len(unis) > 1:
                logging.info(f'Got {len(unis)} universities matching "{uni_name}",\n{unis}\nSelecting best...')
                uni = unis.loc[unis['name'].map(lambda x: len(x.replace(domain, ''))).sort_values().index].iloc[0]

            uni = dict(uni[['name', 'address', 'city', 'state', 'zip', 'country', 'latitude', 'longitude']])
            uni['country'] = US_CODE_TO_COUNTRY[uni['country']]
            uni['domain'] = domain
            domain_unis[domain] = uni
            logging.info(f'Found info for domain "{domain}": {uni}')
            return uni

        # otherwise return the info we have
        loc = _get_geo_location(uni['name'], uni['state-province'], uni['country'])  # try geo-location
        affiliation = uni['name']
        uni = dict(domain=domain, name=affiliation, country=uni['country'], state=uni['state-province'], **loc)
        _register_domain(domain, uni, affiliation)
        return uni

    return None  # no luck


def _process_affiliation():
    affiliation = author['affiliation']
    if affiliation == 'Unknown affiliation':
        return None  # no affiliation entered...

    # searches unis
    for uni in all_unis:
        if uni in affiliation.lower():
            return uni

    # otherwise try to guess affiliation through parsing
    affiliation = re.split(' / | - |,| at ', affiliation)[-1].strip()

    # check if we don't end up with a city or country
    places = GeoText(affiliation)
    if len(places.cities) > 0 and affiliation == places.cities[0] or \
            len(places.countries) > 0 and affiliation == places.countries[0]:
        affiliation = author['affiliation']  # better off keeping full affiliation

    return affiliation


def _register_domain(domain, uni, affiliation):
    domain_unis[domain] = uni
    if domain not in domain_affiliations:
        domain_affiliations[domain] = []
    domain_affiliations[domain].append(affiliation)


def _get_cache(domain, affiliation):
    if domain in domain_unis:
        domain_affiliations[domain].append(affiliation)
        logging.info(f'Domain {domain} fetched from cache')
        return domain_unis[domain]
    return None


def _get_geo_location(affiliation, city, country):
    location = geo_locator.geocode(', '.join(f for f in [affiliation, city, country] if f is not None))
    if location is None:
        return _get_geo_location(None, city, country) if affiliation is not None else \
            _get_geo_location(None, None, country) if city is not None else {}
    loc_country = location.address.split(', ')[-1].lower()
    geo_country = geo_locator.geocode(country).address.split(', ')[-1].lower()
    if loc_country != geo_country:
        return _get_geo_location(None, city, country)  # can't trust affiliation, use only location
    return dict(latitude=location.latitude, longitude=location.longitude, address=location.address)


def _search_author_affiliation():
    # parse email domain
    full_domain = author['email_domain'].lower().replace('@', '')
    domain = '.'.join(full_domain.split('.')[-2:])
    if domain.endswith('.ai') or domain.endswith('.mil'):
        country = 'united states'
    else:
        try:
            country = email2country.email2institution_country(domain)
            if country is not None:
                country = country.lower()
        except SSLError:
            country = None
    affiliation = _process_affiliation()

    # try cache
    uni = _get_cache(full_domain, affiliation)
    if uni is not None:
        return uni

    # try searching US database websites, first sub-domain then domain
    uni = _search_us_unis(full_domain)
    if uni is not None:
        return uni
    # uni = _search_us_unis(domain)
    # if uni is not None:
    #     return uni

    # then search by domain IP address, only full-domain
    uni = _search_domain_ip(full_domain, affiliation, country)
    if uni is not None:
        return uni

    # finally search world universities database, only full-domain
    uni = _search_world_unis(full_domain)
    if uni is not None:
        return uni

    # otherwise just return inferred affiliation and country
    logging.info(f'Could not find info for domain "{domain}": {affiliation}')
    loc = _get_geo_location(affiliation, None, country)  # try geo-location
    uni = dict(domain=full_domain, name=affiliation, country=country, **loc)
    _register_domain(full_domain, uni, affiliation)
    return uni


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--output', type=str, default=OUTPUT_DIR,
                        help='The path to the directory to load and save data.')
    args = parser.parse_args()

    logging.RootLogger.root.handlers = []
    handlers = [logging.FileHandler(os.path.join(args.output, 'locations.log'), 'w', encoding='utf-8'),
                logging.StreamHandler()]
    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S',
                        handlers=handlers)

    # load authors file
    logging.info('==================================================')
    authors_file = os.path.join(args.output, AUTHORS_FILE)
    if not os.path.isfile(authors_file):
        raise ValueError(f'Could not find authors file in "{authors_file}"')
    with open(authors_file, 'r') as fp:
        authors = jsonpickle.loads(fp.read())
    logging.info(f'Loaded info for {len(authors)} authors from "{authors_file}"')

    # load data files
    logging.info('==================================================')

    ip_database = IP2Location.IP2Location(IP_LOCATION_DB, 'SHARED_MEMORY')  # ip 2 location database
    logging.info(f'Loaded IP2Location database from "{IP_LOCATION_DB}"')

    isp_names_df = pd.read_csv(ISP_NAMES_DB, names=['id1', 'id2', 'ip', 'id3', 'name'])  # ip 2 location isp names db
    logging.info(f'Loaded IP2Location names database from "{ISP_NAMES_DB}"')

    with open(WORLD_UNI_DOMAINS, 'r', encoding='utf-8') as fp:
        world_unis = jsonpickle.loads(fp.read())
    logging.info(f'Loaded info for {len(world_unis)} universities from "{WORLD_UNI_DOMAINS}"')

    us_unis_df = pd.read_csv(US_UNI_DATA)
    logging.info(f'Loaded info for {len(us_unis_df)} US universities from "{US_UNI_DATA}"')

    # transform data
    us_unis_df.columns = us_unis_df.columns.str.lower()
    us_unis_df['name'] = us_unis_df['name'].str.lower()
    us_unis_df['alias'] = us_unis_df['alias'].str.lower()
    us_unis_df['website'] = us_unis_df['website'].map(lambda x: re.sub('https?://|www.|/', '', x)).str.lower()

    # gets set of all known universities
    all_unis = set(us_unis_df['name'].unique())
    all_unis.update(uni['name'].lower() for uni in world_unis)

    # set geo-location
    geo_locator = Nominatim(user_agent="uni-finder")

    logging.info('==================================================')
    logging.info('Taking affiliation and location information from each author...')
    domain_unis = {}
    domain_affiliations = {}
    found = total = 0
    for author in tqdm.tqdm(authors.values()):
        name = author['name']
        logging.info(f'Processing {name}...')
        if 'affiliation' not in author or 'email_domain' not in author:
            logging.info(f'No Google Scholar data found for {name}...')
            continue

        uni_data = _search_author_affiliation()
        if uni_data is None:
            logging.info(f'Could not find info for author "{author}"!')
        else:
            found += 1
        total += 1

    logging.info('==================================================')
    logging.info(f'Found {found}/{total} author affiliation locations (total {len(domain_unis)} unique institutes)')

    # correct affiliations, first by known uni name then by majority
    logging.info('Correcting domain affiliations...')
    for domain, affiliations in tqdm.tqdm(domain_affiliations.items()):
        affiliation = None
        for uni in all_unis:
            if any(uni in aff.lower() for aff in affiliations if aff is not None):
                affiliation = uni
                break
        if affiliation is None:
            affiliation = pd.value_counts(affiliations)
            affiliation = affiliation[affiliation == affiliation.max()]
            if len(affiliation) == 0:
                continue
            affiliation = affiliation.index[0]
        domain_unis[domain]['name'] = affiliation

    # save dataframe
    df = pd.DataFrame(domain_unis.values())
    df['name'] = df['name'].map(get_title)
    df['country'] = df['country'].map(get_title)
    df.sort_values(by=['country', 'name', 'domain'], inplace=True)
    df = df[['country', 'name', 'domain', 'latitude', 'longitude', 'address', 'city', 'state', 'zip']]
    file_path = os.path.join(args.output, LOCATIONS_FILE)
    df.to_csv(file_path, index=False, quoting=csv.QUOTE_NONNUMERIC)
    logging.info(f'Saved location data in "{file_path}"')

    logging.info('Done!')
