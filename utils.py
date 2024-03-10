import os
import json
import re
import itertools
import argparse
from concurrent.futures import ThreadPoolExecutor
from xml.etree import ElementTree as ET

import openai
from openai import OpenAI
import tiktoken
import fasttext
fasttext.FastText.eprint = lambda x: None
from opencc import OpenCC
from tenacity import (
    retry,
    stop_after_attempt,
    wait_random_exponential, stop_after_delay, 
    RetryError
)  # for exponential backoff

check_language_type_model = fasttext.load_model("/home/fuyujia/UltraLink/model.bin")
encoding = tiktoken.encoding_for_model('gpt-3.5-turbo')

parser = argparse.ArgumentParser()
parser.add_argument('--wiki_path', '-wi',type=str, default='/data/public/wangshuo/projects/wikipedia/')
parser.add_argument('--question_path', '-qp', type=str, default='/home/fuyujia/data1/form_data/data/') #存问题
parser.add_argument('--dialog_path', '-dp', type=str, default='/home/fuyujia/data1/form_data/dialog/') #存对话
parser.add_argument("--save_interval", "-si", type=int, default=1, help="the interval of saving result")
parser.add_argument("--doc_num", "-dn", type=int, default=1, help="the number of doc that will be processed, zero means all")
parser.add_argument("--split_len", "-sl", type=int, default=2000, help="the length of split text")
parser.add_argument("--max_len", type=int, default=10000, help="the min length of text")
parser.add_argument("--min_len", type=int, default=1000, help="the min length of text")
parser.add_argument("--min_answer_len", "-mal", type=int, default=10, help="the min length of answer")
parser.add_argument('--max_step_len', '-msl', type=int, default=10, help="the max length of random step that chooses the next file")
parser.add_argument('--end_probability', '-ep', type=float,default=0.1, help="the probability of end the dialog, this probability will be doubled when the times of dialog is extended")
parser.add_argument("--num_workers", "-nw", type=int, default=35, help="the number of workers")
parser.add_argument("--prompt_path", "-pp", type=str, default='./prompt.yaml', help="the config of prompt")
parser.add_argument("--generate_without_doc", "-gwd", type=bool, default=False, help="whether generate answer without doc, the default answer will still be generated from doc")
parser.add_argument("--language", "-l", type=str, default='fr', help="the language of the doc")
parser.add_argument("--add_mode", "-am", type=bool, default=False, help="whether add the result to the existed file")

def get_XML(data_path):
#此函数用于读取并解析一个XML文件。
#它首先打开指定路径的文件，读取所有行，并将这些行连接成一个字符串，同时忽略XML声明行（即以<?xml version='1.0' encoding='utf8'?>开头的行）。
#为了确保字符串能被xml.etree.ElementTree库解析，它在字符串的前后分别添加了<root>和</root>标签，从而创建一个包裹原始内容的根节点。
#这是因为XML解析器通常要求XML具有单个根元素。
#最后，使用ET.fromstring(data)将字符串解析为一个ElementTree（元素树）对象，并返回这个对象。
    data = ""
    with open(data_path, 'r') as f:
        lines = f.readlines()
    for line in lines:
        if line.startswith("<?xml version='1.0' encoding='utf8'?>"):
            continue
        data += line
    data = '<root>' + data + '</root>'
    data = ET.fromstring(data)    
    return data

def get_JSON(data_path):
    try:
        with open(data_path, 'r', encoding='utf-8') as f:
            rawStr = f.read()[:-2]  # 去掉最后一个逗号+ \n
            rawStr = '[' + rawStr + ']'
            jsonlist = json.loads(rawStr)
    except:
        jsonlist = []
    return jsonlist

def get_leaf_nodes(directory):
#此函数用于遍历指定目录及其子目录，寻找匹配特定模式的文件。
#这里的模式是通过正则表达式.*wiki_\d.*定义的，意味着函数会查找文件名中包含wiki_后跟一个或多个数字的文件。
#对于每个匹配的文件，它将文件的完整路径添加到一个列表中。遍历是通过os.walk(directory)实现的，这个方法可以遍历目录树中的所有目录和文件。
#最后，函数返回包含所有匹配文件路径的列表。
    leaf_nodes = []
    matcher = re.compile(r'.*wiki_\d.*')
    for dirpath, dirnames, filenames in os.walk(directory):
        for file in filenames:
            if matcher.match(file):
                leaf_nodes.append(os.path.join(dirpath, file))
    return leaf_nodes

    
def get_index(json_path):
#这个函数的目的是从一个JSON文件中获取当前索引值，以便为新加入的数据项分配一个唯一的ID。
    try:
        json_list = json.loads('[' + open(json_path, 'r').read()[:-2] + ']')
        cur_idx = json_list[-1]['id'] + 1
    except:
        cur_idx = 0
    return cur_idx

def get_token_len(txt):
#这个函数用于计算给定文本的令牌长度，即文本经过某种编码处理后的长度。
    return len(encoding.encode(txt))

def write_json(json_list, name, mode = 'a+'):
#这个函数write_json用于将一个列表中的JSON对象写入到一个指定的文件中。它主要用于持久化数据，将内存中的数据结构以JSON格式保存到文件系统中。
    for item in json_list:
        json_str = json.dumps(item, ensure_ascii=False)
        name_dir = os.path.dirname(name)
        try:
            os.makedirs(name_dir)
        except:
            pass
        with open(name, mode, encoding="utf-8") as file:
            file.write(json_str)
            file.write(',\n')
            file.flush()
            os.fsync(file.fileno())

def check_trunk(txt):
#检查给定文本txt经过某种编码（通过encoding.encode方法）后的长度是否达到或超过了一个特定的阈值，这里的阈值设置为4070个令牌。
#这种检查通常用于确定文本是否过长，以便于后续处理，例如在使用自然语言处理模型时避免输入长度超过模型限制。
    txtlen = len(encoding.encode(txt))
    if txtlen < 4070: #4080 有时候可能没有content
        return False
    else:
        return True
    
def check_doc(text, upper_bound=10000, lower_bound=1000, language_type = '__label__zho_Hans'): #'__label__zho_Hans'，这通常指中文简体。
#用于检查给定的文本是否满足特定的条件，包括文本长度和语言类型。
#常用于预处理阶段，确保只有符合要求的文本才会被进一步处理或分析。
    txtlen = len(encoding.encode(text))
    if txtlen < lower_bound or txtlen > upper_bound:
        return False
    t = text.replace('\n', '')
    check_language_type_model = fasttext.load_model("./model.bin")
    text_type = check_language_type_model.predict(t)[0][0]
    if text_type != language_type:
        return False
    return True


def is_title_had_done(title, path, check_dir = None):
#用于检查指定的标题是否已经存在于一个JSON文件中。这种功能通常用于避免重复处理或添加同一个标题的数据，确保数据集的唯一性和一致性。
    if check_dir != None:
        other_path = path.replace('/home/fuyujia/data1/form_data/data/', check_dir)
    else:
        other_path = path
    json_list = get_JSON(other_path)
    if title in [item['title'] for item in json_list]:
        return True
    return False
    
            
def quoter(text, quote='document'):
#用于将给定的文本用特定的标签引用起来。
    return f'<{quote}>' + text + f'<\{quote}>'

def add_comma(file_name):
#在文件的每一行末尾添加逗号和换行符，通常用于准备或修正JSON文件等需要逗号分隔的数据格式。
    with open(file_name, 'r') as f:
        lines = f.readlines()
        lines = [line[:-1] + ',\n' for line in lines]
    
    with open(file_name, 'w') as f:
        f.writelines(lines)
    
def convert_to_simple_chinese(text):
#将中文繁体文本转换为简体文本。
    cc = OpenCC('t2s')
    if isinstance(text, str):
        text = text.encode('utf-8')
    return cc.convert(text)

def get_not_dialog_questions(question_path, dialog_path, language):
#从给定的问题文件中找出那些还没有被转换成对话格式的问题。
#主要用于处理和管理一个包含大量问题的数据集，其中一些问题可能已经被用于生成对话数据，而其他问题尚未使用。
    with open(question_path, 'r') as f:
        questions = f.readlines()
    with open(dialog_path, 'r') as f:
        dialogs = f.readlines()
    questions = [question.strip() for question in questions if language in question]
    questions = set(questions)
    dialogs = ["_".join(dialog.replace("dialog", "data").split('_')[:-1])+".jsonl" for dialog in dialogs if language in dialog]
    #差集操作：函数将问题集合与对话集合进行差集操作，得到那些存在于问题集合中但不在对话集合中的问题，即那些尚未被转换成对话格式的问题。
    dialogs = set(dialogs)
    return list(questions - dialogs) #返回一个列表，包含所有尚未被转换成对话格式的问题。

class ProbabilityIterator:
#一个迭代器用于逐步增加概率值。
#这个类主要用于在迭代过程中动态调整概率值，每次迭代概率值翻倍，直到达到某个阈值或条件。
#随着对话的进行，为了避免对话无限制地延续，系统会通过增加对话结束的概率来促进对话的结束。
#具体来说，每当达到某个条件或阶段（例如，每完成一轮对话），结束对话的概率就会翻倍，从而提高了对话结束的机会。
    def __init__(self, val=0.1):
        self.value = val

    def __iter__(self):
        return self

    def __next__(self):
        value = self.value
        self.value *= 2  
        return value
    
class RequestPool:
    def __init__(self, num_workers=10):
        self.executor = ThreadPoolExecutor(max_workers=num_workers)
        self.keys = [
            "sk-fn62YZPMSqUy6ioV9161C714Fe754885B8122600212d5cA6",
        ]
        self.keys_iter = itertools.cycle(self.keys)
        self.model = "gpt-3.5-turbo"
        self.clients = []
        for k in self.keys:
            client = OpenAI(
                api_key=k,
                base_url = 'https://yeysai.com/v1'
            )
            self.clients.append(client)
        self.clients_iter = itertools.cycle(self.clients)
    
    def commit(self, prompt):
    #接受一个prompt参数，这是一个包含系统提示和用户提示的元组。
    #使用ThreadPoolExecutor的submit方法提交completion_with_backoff方法执行，传入系统提示和用户提示。这允许异步地执行API调用。
        return self.executor.submit(self.completion_with_backoff, prompt[0], prompt[1])
    
    def submit(self, function, *args, **kwargs):
    #将任何函数及其参数提交给线程池执行。这提供了一种灵活的方式来利用线程池执行并发任务。
        return self.executor.submit(function, *args, **kwargs)
    
    
    # 防止调用频率超过每分钟上限的等待代码
    @retry(wait=wait_random_exponential(min=1, max=5), stop=(stop_after_delay(100) | stop_after_attempt(2)))
    # 调用OpenAI API
    def completion_with_backoff(self, system_prompt, user_prompt):
        try:
            # print("sending request")
            client = next(self.clients_iter)
            response = client.chat.completions.create(
                # model="gpt-3.5-turbo-1106",
                model = self.model,
                messages=[
                    {
                        # 系统prompt
                        "role": "system", "content": system_prompt,

                    },
                    {
                        # 每次调用的输入
                        "role": "user", "content": user_prompt,
                    }
                ]
            )
            # API返回回答
            answer = response.choices[0].message.content
            # print("request done")
        except KeyError:
            print("Error in message chat completions.")
            print(json.dumps(response))
            answer = ""
        except Exception as e:
            print(e)
            print("Error in message chat completions.")
            answer = ""
        return answer
        # return f"{result['role']}:{result['content']}"
        