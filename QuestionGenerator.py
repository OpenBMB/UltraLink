import copy
import os
import pdb
import re

import yaml

from tenacity import RetryError

from utils import write_json, quoter, get_index, get_XML, \
    get_token_len, check_doc, is_title_had_done, get_JSON, \
    convert_to_simple_chinese

class QuestionGenerator:
    def __init__(self, args, request_pool) -> None:
        self.request_pool = request_pool
        self.output_path = args.question_path
        self.save_interval = args.save_interval
        self.split_len = args.split_len
        self.max_len = args.max_len
        self.min_len = args.min_len
        self.prompt_path = args.prompt_path
        self.filter_path = args.filter_path if args.filter_path is not None else "default_filter_words.yml"
        self.add_mode = args.add_mode
        self.language = ""
        self.prompt_config = {}
        self.filter_words = ""

        # self.entry_num = args.entry_num #TODO 调整更好的entry方式
        # self.entry_lock = threading.Lock()
        
        if not os.path.exists(self.output_path):
            os.makedirs(self.output_path)
        if not os.path.exists(self.filter_path):
            with open(self.filter_path, 'w') as f:
                pass  # 创建一个空文件
        
    
    def __del__(self):
        del self.request_pool
        
    def set_language(self, language="zh"):
        self.language = language
        self.filter_words = self.load_filter_words(self.filter_path, self.language)
        with open(self.prompt_path, 'r') as f:
            data = yaml.load(f, Loader=yaml.FullLoader)
            for d in data:
                if d['language'] == self.language:
                    self.prompt_config = d
                    break
                
    def construct_data_path(self, data_path):
        # 原始路径中有output，结尾无后缀
        data_path = data_path.split('/')[-4:]
        del data_path[1] # 删除output
        data_path = '/'.join(data_path) + '.jsonl'
        data_path = os.path.join(self.output_path, data_path)
        return data_path
    
    def load_filter_words(self, file_path, language="zh"):
        """根据语言加载filter_word列表"""
        with open(file_path, 'r', encoding='utf-8') as f:
            all_filter_words = yaml.safe_load(f)  # 加载整个文件
            if all_filter_words is None:
                return []  # 文件为空，返回空列表
            else:
                return all_filter_words.get(language, [])  # 获取指定语言的filter_word，如果没有则返回空列表


    def is_filter(self, text):
        """使用正则表达式检查文本是否包含filter_word"""
        #print(self.filter_words)
        if self.filter_words == []:
            return False
        pattern = '|'.join([re.escape(word) for word in self.filter_words])
        #print("构建的正则表达式:", pattern)
        #print(bool(re.search(pattern, text, re.IGNORECASE)))
        return bool(re.search(pattern, text, re.IGNORECASE))
        
    def gene_question(self, data_path):
        file_name = self.construct_data_path(data_path)
        # print(f"Generate questions for {file_name}")
        if self.add_mode == False:
            try:
                with open(os.path.join(self.output_path, "wikiHadDone.txt"), 'r') as file:
                    had_done = file.readlines()
                    if file_name + '\n' in had_done:
                        # print('have done, skip')
                        return file_name
            except:
                pass
            index = get_index(os.path.join(self.output_path, file_name))
        else:
            index = 0
        self.process_doc(index, data_path)
        with open(os.path.join(self.output_path, "wikiHadDone.txt"), 'a+') as file:
            content = file_name + '\n'
            if content not in file.readlines():
                file.write(content)
        return file_name
    
    def create_problem_prompt(self, data_content):
        data_content = quoter(data_content)
        prompt = self.prompt_config['init_question_prompt'] #从对象的 prompt_config 属性中取出 init_question_prompt 键对应的值，赋值给 prompt。
        prompt = prompt.replace(' ', '')
        #pdb.set_trace()  # 在这里设置断点

        return  [prompt + "\n",\
                self.prompt_config["init_question_advice"] + "\n" + self.prompt_config["context_head"] + data_content + "\n" + \
                self.prompt_config["question_head"]]

    def split_text(self, text):
        txtlen = get_token_len(text)
        if txtlen > self.split_len:
            txt_lines = text.split('\n')
            tmp_txt = ''
            txt_list = []
            for i in range(len(txt_lines)):
                if get_token_len(tmp_txt + txt_lines[i]) < self.split_len:
                    tmp_txt += txt_lines[i] + '\n'
                else:
                    txt_list.append(copy.deepcopy(tmp_txt))
                    tmp_txt = ""
            if get_token_len(tmp_txt) > self.min_len:
                txt_list.append(copy.deepcopy(tmp_txt))
        else:
            txt_list = [text]
        return copy.deepcopy(txt_list)
    
    def process_doc(self, index, data_path) -> list:
        data = get_XML(data_path)
        count = 0
        json_list = []
        name = self.construct_data_path(data_path)
        if self.add_mode == False:
            cur_idx = index
        else:
            cur_idx = len(get_JSON(name))

        for doc in data.iter('doc'):
            # print(f"processing {doc.attrib['title']}, cur_idx: {cur_idx}")
            id = cur_idx
            title = doc.attrib['title']
            txt = doc.text
            if self.language == 'zh':
                #print(len(txt))
                txt = convert_to_simple_chinese(txt)
            if self.is_filter(txt):
                #pdb.set_trace()
                continue
            if check_doc(txt, self.max_len, self.min_len, language_type=self.prompt_config['language_type']) == False: 
                continue
            if is_title_had_done(title, name) == True:
                continue
            if index > 0:
                index -= 1
                # print("have done skip")
                continue
            txt_list = self.split_text(txt)

            questions, results = [], []
            
            futures = []
            for txt in txt_list:
                prompt = self.create_problem_prompt(txt)
                futures.append(self.request_pool.commit(prompt))
                
            for future in futures:
                try:
                    result = future.result()
                    result = result.split('\n')
                    result = [r for r in result if len(r) != 0]
                    # Check if result is not empty and language is 'zh' before converting
                    if len(result) > 0 and self.language == 'zh': 
                        #print(result[0])
                        result = [convert_to_simple_chinese(result[0])]
                    questions.append(copy.deepcopy(result))
                except RetryError:
                    questions.append([])


            data_json = {
                'id': id,
                'title': title,
                'txt': txt_list,
                'questions': questions,
            }
            json_list.append(data_json)
            count += 1
            cur_idx += 1
            # with self.entry_lock:
            #     self.entry_num -= 1
            #     if self.entry_num <= 0:
            #         break
            if count >= self.save_interval:
                write_json(json_list, name)
                count = 0
                json_list = []
        write_json(json_list, name)
        return
    