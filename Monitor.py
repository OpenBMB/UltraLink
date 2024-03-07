import argparse
import os
import threading
import fasttext
import random

from DialogGenerator import DialogGenerator
from QuestionGenerator import QuestionGenerator
from utils import get_leaf_nodes, RequestPool, parser, get_not_dialog_questions

class Monitor:
    def __init__(self, args):
        self.wiki_path = args.wiki_path
        self.question_path = args.question_path
        self.dialog_path = args.dialog_path
        self.doc_num = args.doc_num
        self.num_workers = args.num_workers
        self.max_step_len = args.max_step_len
        self.request_pool = RequestPool(num_workers=self.num_workers)
        self.question_generator = QuestionGenerator(args, self.request_pool)
        self.dialog_generator = DialogGenerator(args, self.request_pool)
        
    def start_generate(self):
        languages = os.listdir(self.wiki_path)
        max_volumn = max(1, self.num_workers // 5)# 开太多会把线程全部占据，问答无法使用线程
        for language in languages:
            if language != args.language:
                continue
            self.set_language(language)
            path = os.path.join(self.wiki_path, language)
            leaf_nodes = get_leaf_nodes(path)
            random.shuffle(leaf_nodes)
            node_iter = iter(leaf_nodes)
            futures = []
            count = 0
            if self.doc_num > 0:
                doc_num = args.doc_num
            else:
                doc_num = float("inf")
            no_dialog_list = get_not_dialog_questions(os.path.join(self.question_path, "wikiHadDone.txt"), os.path.join(self.dialog_path, "questionHadDone.txt"), language)
            no_dialog_list = iter(no_dialog_list)
            while True:
                for future in futures:
                    if future.done():
                        dialog_dir = future.result()
                        futures.remove(future)
                        print(f"{dialog_dir} has been done")
                while len(futures) < max_volumn and count < doc_num:
                    try:
                        f = next(no_dialog_list)
                        print(f"the {count} one, start generate {f}")
                        future = self.request_pool.submit(self.dialog_generator.gene_dialog, f)
                        futures.append(future)
                        count += 1
                    except StopIteration:
                        break
                while len(futures) < max_volumn and count < doc_num:
                    if self.doc_num > 0 and count >= self.doc_num:
                        break
                    try:
                        for _ in range(random.randint(1, self.max_step_len)):
                            f = next(node_iter)
                    except StopIteration:
                        break
                    print(f"the {count} one, start generate {f}")
                    future = self.request_pool.submit(self.generate_for_doc, f)
                    futures.append(future)
                    count += 1
                if futures == []:
                    print("all done")
                    break
            
    def generate_for_doc(self, doc_path):
        question_dir = self.question_generator.gene_question(doc_path)
        print(f"question_dir: {question_dir}")
        question_dir = os.path.join(self.question_path, question_dir)
        dialog_dir = self.dialog_generator.gene_dialog(question_dir)
        print(f"dialog_dir: {dialog_dir}")
        return dialog_dir

    def set_language(self, language="zh"):
        self.dialog_generator.set_language(language)
        self.question_generator.set_language(language)

if __name__ == '__main__':
    args = parser.parse_args()
    monitor = Monitor(args)
    monitor.start_generate()