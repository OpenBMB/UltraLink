import ijson
import yaml
import json
import random
import os
import re
import copy
from utils import RequestPool, quoter, check_trunk
from concurrent.futures import as_completed

volume = 20000
worker_num = 30
en_file = "sharegpt/sharegpt.jsonl"
# en_file = "123.jsonl"
out_file = "./sharegpt/sifted_sharegpt.jsonl"
re_list = ["http://", "https://", "www\.", "\.com", "x1", "y1"]

instruction = "Give the analysis and subscore of each criteria and append the total score of the dialogue at the end of the dialogue in the form of 'Total Score: 3'. The analysis should mention the criteria and the dialogue, specify which words or phrases are related to the criteria, and give the score of 1 or 0 for each analysis. The subscore is either 1 or 0. If the analysis indicate where mention related information, give the score 1. In the rest of all situation, give the score 0. Each analysis must followed by a score."
criteria =  """1. Full name of *human*:
Provide a full name of a *human* consisting of 2 part. For example, "Andres Guadamuz", "Donald Trump" and "Peter Park". One part of the full name is not be concerned. For example, "Peter", "Andres" and "Ben". The full name of the specialized term does not belong to the full human name, such as the name of the company, the name of the product, etc.
2.Code snippets and mathematical formulas:
The text is not only a natural language sentence, which includes but is not limited to: programming code snippets, mathematical formulas and variables. Mathematical formulas can include characters beyond "x" or "y". For example, "x+y=3" or "x^2+y^2=1". The usage of numbers is not considered a mathematical formula. For example, "10 years old" and "17%" is not a mathematical formula.
3.Country, region, state, province, city, address:
Specify a particular country, region, state, province, city or address name to uniquely identify the location.
4.Conventions, politics, history and religious:
Integrate conventions, political and religious topics that is only realted to a specific group of people.
5.Poetry, rhymes, mythes, tales, jokes and slangs:
Related to poetry, rhymes, myth, tales, jokes and slangs that reflect the literary and artistic characteristics.
6.Food, cloth, furniture, construction:
Related to traditional food, cloth, furniture and construction that reflect the characteristics of a culture in the aspect of material. Generally, the name of the food, cloth, furniture and construction is not a specialized term. For example, "rice" and "bread" are not specialized terms, but "sushi" and "pizza" are specialized terms.
7.Organization, company, product, brand:
Related to organization, company, product and brand that reflect the characteristics of the organization.
"""
    
def form_dialog(data):
    dialog = ""
    for sentence in data["conversations"]:
        speaker = sentence["from"]
        sentence = sentence["value"]
        dialog = dialog + speaker + ":" + sentence + "\n"
    return copy.deepcopy(dialog)

def form_prompt(data):
    dialog = form_dialog(data)
    user_prompt = ""
    user_prompt += "###\nCriteria:\n" + criteria
    user_prompt += "###\nDialogue:\n" + dialog
    user_prompt += "###\nAnalysis:"
    if not check_trunk(instruction + user_prompt) and all([not re.search(re_str, user_prompt) for re_str in re_list]):
        return copy.deepcopy([instruction, user_prompt])
    else:
        return []

if __name__ == "__main__":
    requestpool = RequestPool(worker_num)
    criteria = criteria.strip()
    with open(en_file, "r") as f:
        en_data = []
        # en_data = json.load(f)
        for line in f:
            en_data.append(json.loads(line))
        en_data = iter(en_data)
    
    try:
        with open(out_file, "r") as f:
            had_done = [json.loads(line) for line in f.readlines()]
    except:
        had_done = []
    had_done = [i['conversations'][1] for i in had_done]
    
    futures = []
    datas = {}
    count = 0
    skip_count = 0
    while len(futures) < min(worker_num, volume):
        try:
            data = copy.deepcopy(next(en_data))
        except StopIteration:
            break
        idx = data['conversations'][1]
        if idx in had_done:
            skip_count += 1
            continue
        
        p = form_prompt(data)
        if len(p) == 0:
            continue
        future = requestpool.commit(p)
        print(f"start {idx}")
        futures.append(future)
        datas[future] = copy.deepcopy(data)
    
    sifted_dialogs = []
    end_count = 0
    failed_count = 0
    success_count = 0
    while True:
        new_futures = []
        for future in as_completed(futures):
            result = future.result()
            data = datas[future]
            if "Total Score: 0" in result:
                sifted_dialogs.append(data)
                print(f"data {data}, result {result}")
                print(f"add {data['id']}")
                success_count += 1
            else:
                print(f"data {data}, result {result}")
                print(f"skip {data['id']}")
                failed_count += 1
            # else:
            #     print(f"error {data['id']}")
            #     print(result)
            del datas[future]
            count += 1
            
            p = []
            try:
                while len(p) == 0:
                    new_data = copy.deepcopy(next(en_data))
                    idx = new_data['conversations'][1]
                    if idx in had_done:
                        skip_count += 1
                        continue
                    p = form_prompt(new_data)
            except StopIteration:
                    continue
                
            new_f = requestpool.commit(p)
            new_futures.append(new_f)
            datas[new_f] = copy.deepcopy(new_data)
            print(f"start {new_data['id']}")
            
        futures = new_futures
        
        if len(sifted_dialogs) > 0:
            with open(out_file, "a+") as f:
                for d in sifted_dialogs:
                    f.write(json.dumps(d) + "\n")
                f.flush()
            sifted_dialogs = []
            
        if len(futures) == 0:
            end_count += 1
        else:
            end_count = 0
        
        if end_count >= 3:
            break