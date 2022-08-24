from elasticsearch import Elasticsearch
import json
import logging
import os
from datetime import datetime
from dotenv import load_dotenv
import fire


def tunnel_to_server():
    command = f'ssh -f -N -q -L "9201:{os.environ["ES_INSTANCE"]}" {os.environ["ES_SERVER"]}'
    logging.info('Tunneling : {}'.format(command))

    os.system("kill $( ps aux | grep '[9]201:' | awk '{print $2}' )")
    os.system(command)
    logging.info("New Elasticsearch URL localhost:9201")
    os.environ['ES_INSTANCE'] = 'localhost:9201'


def composite_search(input_dir: str, tunnel=False):
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
        if tunnel:
            os.system("kill $( ps aux | grep '[9]201:' | awk '{print $2}' )")


if __name__ == "__main__":
    load_dotenv()
    logging.basicConfig(level=logging.INFO)
    # python -m main ./queries/input.json
    fire.Fire(composite_search)
