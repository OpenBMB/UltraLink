import ijson
import yaml
import json
import random
import os
import time
import argparse
import copy
from utils import RequestPool, quoter
from concurrent.futures import as_completed

parser = argparse.ArgumentParser()
parser.add_argument("--volume", type=int, default=100000)
parser.add_argument("--worker_num", type=int, default=1000)
parser.add_argument("--en_file", type=str)
parser.add_argument("--prompt_path" , type=str, default="./multi-sharegpt/sharegpt_prompt.yaml")
parser.add_argument("--languages", type=str, default="es,fr,ru,zh")
parser = parser.parse_args()
# languages = ["ru", "es", "fr"]
languages = parser.languages.split(",")

languages = iter(languages)
volume = parser.volume
worker_num = parser.worker_num
en_file = parser.en_file
prompt_path = parser.prompt_path
save_path = "./multi-sharegpt"
os.makedirs(save_path, exist_ok=True)



def reservoir_sampling(stream, k, had_done):
    reservoir = []
    count = 0
    for i, element in enumerate(stream):
        if element["id"] in had_done:
            continue
        count = count + 1
        if count <= k:
            reservoir.append(element)
        else:
            probability = k / (count + 1)
            if random.random() < probability:
                 reservoir[random.choice(range(k))] = element
    return reservoir

if __name__ == "__main__":
    for lan in languages: 
        fail_count = 0   
        out_file = os.path.join(save_path, f"sharegpt_{lan}.json")
        try:
            with open(out_file, "r") as f:
                had_done = [json.loads(line) for line in f.readlines()]
        except:
            had_done = []
        had_done = [i['id'] for i in had_done]
        with open(en_file, "r") as f:
            en_data = [json.loads(line) for line in f.readlines()]
            en_data = iter(en_data)
            sampled_data = reservoir_sampling(en_data, volume, had_done)
            en_data = iter(sampled_data)

        with open(prompt_path, 'r') as f:
            data = yaml.load(f, Loader=yaml.FullLoader)
            for d in data:
                if d['language'] == lan:
                    prompt1 = d['prompt1']
                    prompt2 = d['prompt2']
                    text = d['text']
                    translation = d['translation']
                    break
        requestpool = RequestPool(worker_num)
        waiting_data = []
        finished_data = []
        index_list = {}
        while True:   
            for i in range(10):
                try:
                    j = next(en_data)
                except StopIteration:
                    fail_count = 1
                    break
                r = {}
                r['id'] = j['id']
                r["original_conversations"] = j["conversations"]
                r["conversations"] = copy.deepcopy(j["conversations"])
                r['futures'] = []
                for index, dialog in enumerate(r["conversations"]):
                    prompt = [prompt1, text + '\n' + dialog["value"] + "\n" + translation]
                    dialog["value"] = ""
                    future = requestpool.commit(prompt)
                    print(f"start {j['id']} {index}")
                    r['futures'].append(future)
                    index_list[future] = index
                waiting_data.append(r)
            
            for r in waiting_data:
                for future in as_completed(r['futures']):
                    index = index_list[future]
                    r['conversations'][index]['value'] = future.result()
                    print(f"finish {r['id']} {index}")
                    index_list.pop(future)
                if all([i['value'] != "" and i['value'] is not None for i in r['conversations']]):
                    del r['futures']
                    finished_data.append(r)
                else:
                    pass
            waiting_data = []
                
                
            if len(finished_data) >= 1:
                with open(out_file, "a+") as f:
                    for r in finished_data:
                        f.write(json.dumps(r, ensure_ascii=False) + "\n")
                    f.flush()
                    finished_data = []
            
            if fail_count == 1:
                break