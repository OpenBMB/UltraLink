import ijson
import yaml
import json
import random
import os
import argparse
from utils import RequestPool, quoter
from concurrent.futures import as_completed

parser = argparse.ArgumentParser()
parser.add_argument("--volume", type=int, default=200)
parser.add_argument("--worker_num", type=int, default=200)
parser.add_argument("--en_file", type=str)
parser.add_argument("--prompt_path" , type=str, default="./multi-math/math_prompt.yaml")
parser.add_argument("--languages", type=str, default="ja")
parser = parser.parse_args()
# languages = ["ru", "es", "fr"]
languages = parser.languages.split(",")

languages = iter(languages)
volume = parser.volume
worker_num = parser.worker_num
en_file = parser.en_file
prompt_path = parser.prompt_path
save_path = "./multi-math"
os.makedirs(save_path, exist_ok=True)



def reservoir_sampling(stream, k, had_done):
    reservoir = []
    count = 0
    for i, element in enumerate(stream):
        if i in had_done:
            continue
        count = count + 1
        if count <= k:
            reservoir.append((i,element))
        else:
            probability = k / (count + 1)
            if random.random() < probability:
                 reservoir[random.choice(range(k))] = (i,element)
    return reservoir

if __name__ == "__main__":
    for lan in languages: 
        fail_count = 0   
        out_file = os.path.join(save_path, f"MetaMathQA_{lan}.json")
        try:
            with open(out_file, "r") as f:
                had_done = [json.loads(line) for line in f.readlines()]
        except:
            had_done = []
        had_done = [i['id'] for i in had_done]
        with open(en_file, "r") as f:
            en_data = ijson.items(f, 'item')
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
                idx, j = next(en_data)
            except StopIteration:
                break
            if idx in had_done:
                continue
            r = {}
            r['id'] = idx
            r['type'] = j['type']
            r['original_question'] = j['original_question']
            r['original_query'] = j['query']
            r['original_response'] = j['response']
            p = [prompt1, text + '\n' + quoter(j['query'], quote="text") + translation]
            print(f"start {idx}")
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
                if len(p) == 0:
                    del data[future]
                    continue
                # print(p)
                # print()
                if t == 0:
                    r['query'] = p
                    p = [prompt2, text + '\n' + quoter(j['response'], quote="text") + translation]
                    print(f"get query {r['id']}")
                    # print(p[1])
                    # print()
                    f = requestpool.commit(p)
                    new_futures.append(f)
                    data[f] = (r, j, 1)
                    del data[future]
                else:
                    r['response'] = p
                    result.append(r)
                    print(f"done {r['id']}")
                    del data[future]
                    try:
                        idx, j = next(en_data)
                    except StopIteration:
                        continue
                    if idx in had_done:
                        continue
                    r = {}
                    r['id'] = idx
                    r['type'] = j['type']
                    r['original_question'] = j['original_question']
                    r['original_query'] = j['query']
                    r['original_response'] = j['response']
                    p = [prompt1, text + '\n' + quoter(j['query'], quote="text") + translation]
                    print(f"start {idx}")
                    # print(p[1])
                    # print()
                    future = requestpool.commit(p)
                    new_futures.append(future)  
                    data[future] = (r, j, 0)
            futures = new_futures
                
            if len(result) >= 1:
                with open(out_file, "a+") as f:
                    for r in result:
                        f.write(json.dumps(r, ensure_ascii=False) + "\n")
                    f.flush()
                    result = []

            if len(futures) == 0:
                fail_count = fail_count + 1
            else:
                fail_count = 0
            
            if fail_count > 5:
                break