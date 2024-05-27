# CASS Fiction EPUB Body Extraction #
# Institution: Lancaster University #
# Author: Hanna Schmueck #
# Contact: h.schmueck@lancaster.ac.uk #

import os
import re
from tqdm import tqdm
import pandas as pd
from bs4 import BeautifulSoup

class fictBodyExtraction:
    def __init__(self, folder_path):
        self.folder_path = folder_path
        self.txtfilelist = []
        self.epubfilelist = []
        self.splitfiles = {}
        self.tooshortfiles = {}

    def get_files(self):
        for path, subdirs, files in os.walk(self.folder_path):
            for file in files:
                if file.endswith(".txt") or file.endswith('.TXT'):
                    self.txtfilelist.append(os.path.join(path, file))
                if file.endswith(".epub") or file.endswith('.EPUB') or file.endswith('.Epub'):
                    self.epubfilelist.append(os.path.join(path, file))

    def chapter_to_str(chapter):
        soup = BeautifulSoup(chapter.get_body_content(), 'html.parser')
        text = [para.get_text() for para in soup.find_all('p')]
        return ' '.join(text)

    def startchecker(needles, haystackindex, paragraphs):
        window_size = 20
        testing = ' '.join(paragraphs[haystackindex: haystackindex + window_size]).lower()
        beginning = ' '.join(paragraphs[haystackindex: haystackindex + 5]).lower()
        if len(testing) > window_size*80 and len(beginning)> 400:
            if not any(substring in testing for substring in needles):
                return haystackindex

    def endchecker(needles, haystackindex, paragraphs):
        window_size = 5
        testing = ' '.join(paragraphs[haystackindex:haystackindex+window_size]).lower()
        if len(testing) > window_size*100:
            if not any(substring in testing for substring in needles):
                return haystackindex+window_size

    def split_body(self):
        splitfiles = {}
        tooshortfiles ={}

        beginnings = r'(\n[\s\-\*\#]*((CHAPTER[\s\-\*\#]*(I|1|One))|(part[\s\-\*\#]*(1|I|One))|(FIRST[\s\-\*\#]*CHAPTER)|(First[\s\-\*\#]*part)|(preface)))|(([\s\-\*\#]+(INTRODUCTION|ONE|I\.|1\.)[\s\-\*\#]*\n)|(Prologue[\s\-\*\#]*\n)|([\s\-\*\#]*Episode \d+.{0,15}\n)|(\n\*\*1\*\*\n))'
        ends = r'(\n[\s\-\*\#]*(glossary|acknowledgements|BIBLIOGRAPHY|THE END|recipes)[\s\-\*\#]*\n)|(((Author.?s Note)|(\*\*Table of Contents\*\*)|(This is a work of fiction)|(post-?script)|(epilogue)|(ACKNOWLEDGMENTS)|(\*What\'s next)|(About the Author)|(Other .{3,30} by .{1,50})[\s\-\*\#]*\n))'
        contents =  r'(TOC)|(contents)[\s\*\#]*\n(.{1,50}\n{1,3}){5,}'

        forbiddencontents = ["ABOUT THE AUTHOR","ALSO BY ", 'ISBN: 978', 'Jacket image by', 'is the author', 'contents','Times Book Review','sign up for our newsletters','Book Club pick','the publisher does not',
        "Praise for", 'acknowledgements', 'writing of this book', 'Independent Publishers', 'ACKNOWLEDGMENTS', 'book design', "What's next on",'for more information about ','about the authors',
        "©", '### available from', 'e-book', 'Printed in', 'is dedicated to', "Children's Books supports the First Amendment",'MEET THE AUTHOR'
        "This is a work of fiction", "right to reproduce", "Translation copyright", "all rights reserved", "ISBN 97", "are registered trademarks of", "cover design by", "any resemblance to actual persons", "AUTHOR'S NOTE", "this novel is a work of fiction", 'Books by', 
        "Version\_","TRADEMARK REGISTERED","All Rights Reserved.","Copyright infringement is against the law.",'**CONTENTS**', '# Contents', 'jacket art by', 'events in this book are fictitious', 'to persons living or dead', '—*Booklist*', 'to the memory of ', 'previously published in', 'TITLES BY MERCEDES LACKEY']
        forbiddencontents = [i.lower() for i in forbiddencontents]


        for file in tqdm(self.txtfilelist):
            with open(str(file), 'r+', encoding='utf-8') as inf: 
                text = inf.read()
                text = re.sub(r"[\s\*\#]+\n(.{1,50}\n{1,3}){5,}", " ",text, 0, re.IGNORECASE)
                text = text.replace(u"\u002D\u00ad", '')
                text = text.replace(u"\u00ad", '')
                text = re.sub(r"[\u2018\u2019\u201C\u201D\u2039\u203A`’\‘’“”‹›•']", "'", text, 0, re.MULTILINE)
                text = re.sub(r"\n{2,}", "\n", text, 0, re.MULTILINE | re.DOTALL)
                text = re.sub(r"[\r\t\f\v  ]", " ", text, 0, re.MULTILINE | re.DOTALL)
                text = re.sub(r" {2,}", " ", text, 0, re.MULTILINE | re.DOTALL)
                pretext = ''
                mainandend = ''
                posttext = ''
                main=''
                last25percent = ''
                first75percent = ''

                if len(text.split(' ')) < 30000:
                    splitfiles[file.split('/')[-1]] = ['tooshort', len(text.split(' ')),0,0,0]

                else:
                    b_textparts  = re.split(beginnings, text, maxsplit=1, flags= re.I)

                    if len(b_textparts) > 1:
                        pretext = b_textparts[0]
                        mainandend = b_textparts[-1]
                        b_status = 'fine'
                        
                    else:
                        paragraphs = text.split('\n')
                        paragraphs = [i for i in paragraphs if i]
                        window_size = 10
                        for i in range(len(paragraphs) - window_size + 1):
                            breakindex = self.startchecker(forbiddencontents, i, paragraphs=paragraphs)
                            if breakindex:
                                pretext = '\n'.join(paragraphs[:breakindex+1])
                                mainandend = '\n'.join(paragraphs[breakindex+1:])
                                break
                            else:
                                continue

                    last25percent = mainandend[round(0.75*len(mainandend)):]
                    first75percent = mainandend[:round(0.75*len(mainandend))]

                    e_textparts = re.split(ends, last25percent, flags= re.I)
                    if len(e_textparts) > 1:
                        main = [textpart for textpart in e_textparts[:-1] if textpart]
                        main = ' '.join(map(str, main))
                        main = first75percent+ ' '+ main
                        posttext = e_textparts[-1]
                        e_status = 'fine'

                    else:
                        paragraphs = last25percent.split('\n')
                        paragraphs = [i for i in paragraphs if i]
                        window_size = 10
                        for i in reversed(range(len(paragraphs) - window_size + 1)):
                            breakindex = self.endchecker(forbiddencontents, i, paragraphs=paragraphs)
                            if breakindex:
                                main = first75percent+ '\n'.join(paragraphs[:breakindex-1])
                                posttext = '\n'.join(paragraphs[breakindex-1:])
                                e_status = 'fine'
                                break
                            else:
                                continue
                    main = main.replace('*', "")
                    main = main.replace('#', ' ')
                    main = main.replace('~', ' ')
                    main = re.sub(r" {2,}", " ", main, 0, re.MULTILINE | re.DOTALL)
                        
                    splitfiles[file] = [b_status, e_status,pretext,main,posttext]

        split = pd.DataFrame(splitfiles).T
        split = split.rename(columns={0: "b_status", 1: "e_status", 2:'pretext', 3: 'main', 4: 'posttext'})
        split = split.reset_index()
        split = split.rename(columns={'index':'path'})
        return split