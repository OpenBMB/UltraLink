import os
import copy
import argparse
import yaml
import random
import pdb

from utils import write_json, quoter, get_index, get_leaf_nodes,\
    get_JSON, RequestPool, check_trunk, ProbabilityIterator, get_token_len,\
    convert_to_simple_chinese

class DialogGenerator:
    def __init__(self, args, request_pool):
        self.request_pool = request_pool
        self.output_path = args.dialog_path
        self.save_interval = args.save_interval
        self.end_probability = args.end_probability
        self.prompt_path = args.prompt_path
        self.min_answer_len = args.min_answer_len
        self.add_mode = args.add_mode
        self.language = ""
        self.is_generate_without_doc = args.generate_without_doc
        self.prompt_config = {}
                
        if not os.path.exists(self.output_path):
            os.makedirs(self.output_path)

    def __del__(self):
        del self.request_pool
        
    def set_language(self, language):
        self.language = language
        with open(self.prompt_path, 'r') as f:
            data = yaml.load(f, Loader=yaml.FullLoader)
            for d in data:
                if d['language'] == self.language:
                    self.prompt_config = d
            
    def create_init_answer_prompt(self, context, question):
        context = quoter(context)
        question = question
        #print(question)

        prompt = self.prompt_config['init_answer_prompt']
        prompt = prompt.replace(" ", "")
        return  [prompt, \
                self.prompt_config["context_head"] + context + "\n" + \
                self.prompt_config["question_head"] + question  + "\n" + \
                self.prompt_config["answer_head"]]
        
    def create_init_answer_without_context_prompt(self, question):
        question = question
        prompt = self.prompt_config['init_answer_prompt']
        prompt = prompt.replace(" ", "")
        # prompt = ""
        return  [prompt, \
                self.prompt_config["question_head"] + question  + "\n" + \
                self.prompt_config["answer_head"]]
    
    def create_depth_question_prompt(self, context, dialog):
        context = quoter(context)
        dialog = dialog
        prompt = self.prompt_config['depth_question_prompt']
        prompt = prompt.replace(" ", "")
        return  [prompt, \
                self.prompt_config["depth_question_advice"] + "\n" + \
                self.prompt_config["context_head"] + context + "\n" + \
                self.prompt_config["dialog_head"] + dialog  + "\n" + \
                self.prompt_config["question_head"]]
    
    def create_width_question_prompt(self, context, dialog):
        context = quoter(context)
        dialog = dialog
        prompt = self.prompt_config['width_question_prompt']
        prompt = prompt.replace(" ", "")
        return  [prompt,\
                self.prompt_config["width_question_advice"] + "\n" + \
                self.prompt_config["context_head"] + context + "\n" + \
                self.prompt_config["dialog_head"] + dialog  + "\n" + \
                self.prompt_config["question_head"]]
    
    def create_following_answer_prompt(self, context, question, dialog):
        context = quoter(context)
        question = question
        dialog_txt = self.convert_dialog(dialog)
        prompt = self.prompt_config['following_answer_prompt']
        prompt = prompt.replace(" ", "")
        # prompt = ""
        return  [prompt, \
                self.prompt_config["context_head"] + context + "\n" + \
                self.prompt_config["dialog_head"] + dialog_txt + \
                self.prompt_config["question_head"] + question  + "\n" + \
                self.prompt_config["answer_head"]]
        
    def create_following_answer_without_context_prompt(self, question, dialog):
        question = question
        dialog_txt = self.convert_dialog(dialog)
        prompt = self.prompt_config['following_answer_prompt']
        prompt = prompt.replace(" ", "")
        # prompt = ""
        return  [prompt, \
                self.prompt_config["dialog_head"] + dialog_txt + \
                self.prompt_config["question_head"] + question  + "\n" + \
                self.prompt_config["answer_head"]]

    def create_question_prompt(self, context, dialog):
        random_num = random.randint(0, 1)
        dialog_txt = self.convert_dialog(dialog)
        if random_num == 0:
            return self.create_depth_question_prompt(context, dialog_txt)
        else:
            return self.create_width_question_prompt(context, dialog_txt)
        
    def convert_dialog(self, dialog): # 从列表转换成一段话
        txt = ''
        for q, a in dialog:
            txt += self.prompt_config["question_head"] + q + '\n' +\
                    self.prompt_config["answer_head"] + a + '\n'
        return txt

    
    def gene_dialog(self, data_path):
        # 这里路径没有output，和construct_data_path不同
        file_name = self.construct_data_path(data_path)
        # print(f"Generate dialog for {file_name}")
        if self.add_mode == False:
            try:
                with open(os.path.join(self.output_path, "questionHadDone.txt"), 'r') as file:
                    had_done = file.readlines()
                    if file_name + '\n' in had_done:
                        # print('have done, skip')
                        return file_name
            except:
                pass
            index = get_index(os.path.join(self.output_path, file_name))

        else:
            index = 0
        self.process_doc(data_path, index)
        with open(os.path.join(self.output_path, "questionHadDone.txt"), 'a+') as file:
            content = file_name + '\n'
            if content not in file.readlines():
                file.write(file_name+'\n')
            # print(f'write {file_name} to questionHadDone.txt')
        return file_name
    
    def whether_to_continue(self, iterator):
        prob = next(iterator)
        if random.random() > prob:
            return True
        else:
            return False
        
    def gene_dialog_from_txt(self, txt, questions):
            
        iterator = ProbabilityIterator(self.end_probability)
        for question in questions: #拿第一个非0 question
            if question == '':
                continue
            break
        if questions == []:
            return []
        subdialog = []

        prompt = self.create_init_answer_prompt(txt, question)
        if(check_trunk("".join(prompt))):
            return subdialog
        
        answer = self.request_pool.commit(prompt).result()
        print("get answer")
        if(check_trunk("".join(prompt) + answer)):
            return subdialog
        elif len(answer) < self.min_answer_len:
            return subdialog
        elif len(answer) == 0: # 被过滤掉了
            return subdialog
        
        subdialog.append(copy.deepcopy([question, answer]))
        
        isContinue = True
        while (isContinue):
            prompt = self.create_question_prompt(txt, subdialog)
            if(check_trunk("".join(prompt))):
                isContinue = False
                continue
            question = self.request_pool.commit(prompt).result()
            if(check_trunk("".join(prompt) + question)):
                isContinue = False
                continue
            
            prompt = self.create_following_answer_prompt(txt, question, subdialog)
            if(check_trunk("".join(prompt))):
                isContinue = False
                continue
            answer = self.request_pool.commit(prompt).result()
            print("get answer")
            if(check_trunk("".join(prompt) + answer)):
                isContinue = False
                continue
            elif len(answer) < self.min_answer_len:
                isContinue = False
                continue
            
            subdialog.append(copy.deepcopy([question, answer]))
            isContinue = self.whether_to_continue(iterator)
        
        return copy.deepcopy(subdialog)
    
    def gene_dialog_without_txt(self, questions):
        iterator = ProbabilityIterator(self.end_probability)
        questionIterator = iter(questions)
        subdialog = []
        try:       
            question = next(questionIterator)
        except:
            return subdialog
        prompt = self.create_init_answer_without_context_prompt(question)
        if(check_trunk("".join(prompt))):
            return subdialog
        
        answer = self.request_pool.commit(prompt).result()
        if(check_trunk("".join(prompt) + answer)):
            return subdialog
        elif len(answer) < self.min_answer_len:
            return subdialog
        elif len(answer) == 0: # 被过滤掉了
            return subdialog
        
        subdialog.append(copy.deepcopy([question, answer]))
        
        isContinue = True
        while (isContinue):
            try:
                question = next(questionIterator)
            except:
                break
            prompt = self.create_following_answer_without_context_prompt(question, subdialog)
            if(check_trunk("".join(prompt))):
                isContinue = False
                continue
            answer = self.request_pool.commit(prompt).result()
            if(check_trunk("".join(prompt) + answer)):
                isContinue = False
                continue
            elif len(answer) < self.min_answer_len:
                isContinue = False
                continue
            elif len(answer) == 0:
                return subdialog
            
            subdialog.append(copy.deepcopy([question, answer]))
            isContinue = self.whether_to_continue(iterator)
        
        return copy.deepcopy(subdialog)
                    
    def construct_data_path(self, data_path):
        name = data_path.split('/')[-3:]
        name = '/'.join(name).split(".")[0] + '_dialog.jsonl'
        name = os.path.join(self.output_path, name)
        return name
    
    def construct_data_path_without_txt(self, data_path):
        name = data_path.split('/')[-3:]
        name = '/'.join(name).split(".")[0] + '_dialog_without_txt.jsonl'
        name = os.path.join(self.output_path, name)
        return name
    
    def process_doc(self, data_path, index = 0) -> list:
        jsonlist = get_JSON(data_path)
        count = 0
        dialog_list = []
        if self.is_generate_without_doc:
            dialog_without_doc_list = []
        for item in jsonlist:
            id = item['id']
            txt = item['txt']
            if self.language == 'zh':
                txt = [convert_to_simple_chinese(t) for t in txt]
            questions = item['questions']
            if self.language == 'zh':
                for seq_q in questions:
                    for q in seq_q:
                        q = convert_to_simple_chinese(q)

            dialog = {}
            dialog["id"] = id
            dialog['txt'] = txt
            dialog['dialogs']= []
            
            had_done_dialog = get_JSON(self.construct_data_path(data_path))
            ids = [d['id'] for d in had_done_dialog]
            
            if self.is_generate_without_doc:
                dialog_without_doc = {}
                dialog_without_doc["id"] = id
                dialog_without_doc['dialogs']= []
                
            if id < index:
                # print('have done, skip')
                continue
            if self.add_mode:
                if id in ids:
                    continue
            
            futures = []
            for i in range(len(questions)):
                futures.append(self.request_pool.submit(self.gene_dialog_from_txt, txt[i], questions[i]))
                
            for f in futures:
                result = f.result()
                if result:  # Ensure non-empty results are appended
                    dialog['dialogs'].append(result)
                    if self.is_generate_without_doc:
                        questions_without_txt = [q for q, _ in result]
                        dialog_without_doc_result = self.gene_dialog_without_txt(questions_without_txt)
                        if dialog_without_doc_result:  # Check for non-empty results before appending
                            dialog_without_doc['dialogs'].append(dialog_without_doc_result)

            if dialog['dialogs']:  # Only append if 'dialogs' is not empty
                dialog_list.append(dialog)
            if self.is_generate_without_doc and dialog_without_doc['dialogs']:  # Check for non-empty before appending
                dialog_without_doc_list.append(dialog_without_doc)

            count += 1
            
            if self.save_interval > 0 and count >= self.save_interval:
                name = self.construct_data_path(data_path)
                write_json(dialog_list, name)
                if self.is_generate_without_doc:
                    name2 = self.construct_data_path_without_txt(data_path)
                    write_json(dialog_without_doc_list, name2)
                    dialog_without_doc_list = []
                count = 0
                dialog_list = []
                print(f"Save {name}")
        name = self.construct_data_path(data_path)
        write_json(dialog_list, name)
        if self.is_generate_without_doc:
            name2 = self.construct_data_path_without_txt(data_path)
            write_json(dialog_without_doc_list, name2)
        print(f"Save {name}") 