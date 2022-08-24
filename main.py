from elasticsearch import Elasticsearch
import json
import logging
import os
from datetime import datetime
from dotenv import load_dotenv
import fire
import pandas as pd


def tunnel_to_server():
    """
    To run the script locally but querying form a remote server an ssh tunnel needs to be created
    :return:
    """
    command = f'ssh -f -N -q -L "9201:{os.environ["ES_INSTANCE"]}" {os.environ["ES_SERVER"]}'
    logging.info('Tunneling : {}'.format(command))

    # kill tunnel if it already exists
    os.system("kill $( ps aux | grep '[9]201:' | awk '{print $2}' )")
    os.system(command)
    logging.info("New Elasticsearch URL localhost:9201")
    os.environ['ES_INSTANCE'] = 'localhost:9201'


def composite_search(input_dir: str, tunnel=False):
    """

    :param input_dir: path to json input query
    :param tunnel: if true will extract from remote server afer creating an ssh tunnel
    :return:
    """
    try:
        if tunnel:
            tunnel_to_server()
        connection = Elasticsearch(f'http://{os.environ["ES_INSTANCE"]}', request_timeout=120)

        with open(input_dir, 'r', encoding='utf-8') as f:
            query = json.loads(f.read())

        total_hits = 0
        with open(f'response/output-{datetime.now().strftime("%Y%m%d-%H%M%S")}.json', 'a+', encoding='UTF-8') as out:
            out.write("[")
            while True:
                res = connection.search(index=os.environ['INDEX'], query=query['query'], aggs=query['aggs'])

                for hit in res['aggregations']['two']['buckets']:
                    if total_hits > 0:
                        out.write(",\n")

                    out.write(json.dumps(hit))
                    total_hits += 1

                if "after_key" not in res['aggregations']['two']:
                    out.write("]")
                    break

                logging.info('new batch {}'.format(res['aggregations']['two']['after_key']))
                after_dict = res['aggregations']['two']['after_key']
                query['aggs']['two']["composite"]["after"] = after_dict

            logging.info(total_hits)

    except Exception as e:
        logging.error(e)
    finally:
        # close ssh tunnel if opened
        if tunnel:
            os.system("kill $( ps aux | grep '[9]201:' | awk '{print $2}' )")


def merge_google_data(google_dir: str, elastic_dir: str):
    """

    :param google_dir: path to csv file containing google search data
    :param elastic_dir: path to csv file containing exported data
    :return:
    """
    google = pd.read_csv(google_dir)
    kibana = pd.read_csv(elastic_dir)
    merged = kibana.merge(google, left_on='url', right_on='Landing Page')
    merged.to_csv(f'./SEO-bucket-prices-{datetime.now().strftime("%Y%m%d-%H%M%S")}.csv')


if __name__ == "__main__":
    # Extracting json fields to csv with column names
    # cat output-20220824-111835.json | jq -r '["url","train","bus","plane"], (.[] | [.key.comp,.four.value,.five.value,.six.value]) | @csv' > output-20220824-111835.csv
    load_dotenv()
    logging.basicConfig(level=logging.INFO)
    fire.Fire({'composite-search': composite_search,
               'merge-data': merge_google_data})
