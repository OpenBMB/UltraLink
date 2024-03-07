import ijson
import yaml
import json
import random
import os
import re
import argparse
from utils import RequestPool, quoter
from concurrent.futures import as_completed

parser = argparse.ArgumentParser()
parser.add_argument("--volume", type=int, default=500)
parser.add_argument("--worker_num", type=int, default=500)
parser.add_argument("--en_file", type=str)
parser.add_argument("--prompt_path" , type=str, default="./humaneval/prompt.yaml")
parser.add_argument("--languages", type=str, default="fr,es,ru")
parser = parser.parse_args()
# languages = ["ru", "es", "fr"]
languages = parser.languages.split(",")
matcher = re.compile(r"(```.*?```)", re.DOTALL)

languages = iter(languages)
volume = parser.volume
worker_num = parser.worker_num
en_file = parser.en_file
prompt_path = parser.prompt_path
save_path = "./humaneval/"
os.makedirs(save_path, exist_ok=True)

def reservoir_sampling(stream, k, had_done):
    reservoir = []
    count = 0
    for i, element in enumerate(stream):
        if element["task_id"] in had_done:
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
        out_file = os.path.join(save_path, f"humaneval_{lan}.jsonl")
        try:
            with open(out_file, "r") as f:
                had_done = [json.loads(line) for line in f.readlines()]
        except:
            had_done = []
        had_done = [i['task_id'] for i in had_done]
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
        result = []
        futures = []
        count = 0
        data = {}
        while len(futures) < min(worker_num, volume):
            try:
                j = next(en_data)
            except StopIteration:
                print("no data")
                fail_count = 1
                break
            r = {}
            r['task_id'] = j['task_id']
            r['prompt'] = ''
            r['entry_point'] = j['entry_point']
            r['canonical_solution'] = j['canonical_solution']
            r['test'] = j['test']
            p = ["", prompt1 + j['prompt'] + translation]
            print(f"start {j['task_id']}")
            # print(p[1])
            # print()
            future = requestpool.commit(p)
            futures.append(future)  
            data[future] = (r, j, 0)
            
        while True:   
            new_futures = []
            for i, future in enumerate(as_completed(futures)):
                # print(i)
                r, j, t = data[future]
                p = future.result()  
                if p == None or len(p) == 0 or p == "" :
                    pass
                else:
                # print(p)
                # print()
                    r['prompt'] = p
                    result.append(r)
                    print(f"done {r['task_id']}")
                del data[future]
                try:
                    j = next(en_data)
                except StopIteration:
                    fail_count = 1
                    continue
                while j['id'] in had_done:
                    try:
                        j = next(en_data)
                    except StopIteration:
                        fail_count = 1
                        break
                r = {}
                r['task_id'] = j['task_id']
                r['prompt'] = ''
                r['entry_point'] = j['entry_point']
                r['canonical_solution'] = j['canonical_solution']
                r['test'] = j['test']
                p = [prompt1, text + '\n' + j['prompt'] + '\n' + translation]
                print(f"start {j['task_id']}")
                # print(p[1])
                # print()
                future = requestpool.commit(p)
                new_futures.append(future)  
                data[future] = (r, j, 0)
            futures= new_futures
                
            if len(result) >= 1:
                with open(out_file, "a+") as f:
                    for r in result:
                        f.write(json.dumps(r, ensure_ascii=False) + "\n")
                    f.flush()
                    result = []

            if fail_count == 1:
                break
            
            
            
        