import os
import sys
import zlib
import time
import spacy
import humanize
import numpy as np
import pandas as pd
import pickle as pkl
from tqdm import tqdm
from datetime import timedelta
from bs4 import BeautifulSoup as bs4
from sklearn.linear_model import LinearRegression
from concurrent.futures import ProcessPoolExecutor



class spacyPipeline:

    def __init__(self, corpus_title="SpaCy Pipeline Corpus", file_type="Auto", multi_process=False, worker_nodes=1, 
                 attributes=['lemma', 'pos', 'tag', 'dep', 'shape', 'is_alpha', 'is_stop', 'pymusas'], 
                 warnings=True, metadata=True, metadata_file="metadata/metadata.csv", metadata_id_column="ID", data_dir="data", 
                 all_files=[], single_file_type=True, multi_filetypes=[], output_dir="output", create_output_folder=False,
                 use_nonempty_output_folder = False, flat_output_dir=False, xml_text_node="text", xml_metadata_node="text",
                 spacy_features=True, errors="strict", start_benchmark=False, benchmark_sample=20, compress=False, skip_processed_files=False, **kwargs):
        
        self.corpus_name = corpus_title
        self.compress = compress
        self.skip_processed_files = skip_processed_files
        self.static_ID = 0
        self.errors = errors
        self.metadata = metadata
        self.benchmark_sample = benchmark_sample
        self.start_benchmark = start_benchmark
        self.init_status = True
        self.spacy_features = spacy_features
        self.output_dir = output_dir
        self.metadata_id_column = metadata_id_column
        self.flat_output_dir = flat_output_dir
        self.xml_text_node = xml_text_node
        self.xml_metadata_node = xml_metadata_node
        self.multi_process = multi_process
        self.worker_nodes = worker_nodes
        self.metadata = metadata
        self.attributes = attributes
        self.warnings = []

        # Initialise SpaCy + Pymusas

        if spacy_features:
            self.nlp = spacy.load('en_core_web_trf')
            self.nlp.max_length = 100000000
            english_tagger_pipeline = spacy.load('en_dual_none_contextual')
            self.nlp.add_pipe('pymusas_rule_based_tagger', source=english_tagger_pipeline)

        # Parameter Integrity Checks

        # Files

        self.valid_files = []
        if isinstance(all_files, list) and len(all_files) > 0:
            for file in all_files:
                if os.path.exists(file):
                    self.valid_files.append(file)
                else:
                    self.warnings.append(f"File does not exist: {file}")
        elif not isinstance(data_dir, str):
            self.warnings.append(f"Directory provided is invalid datatype: {type(data_dir)}")
        elif os.path.exists(data_dir):
            for dirs, paths, files in os.walk(data_dir):
                for file in files:
                    self.valid_files.append(os.path.join(dirs, file))
            if len(self.valid_files) == 0:
                self.warnings.append("No files available to process")
                self.init_status = False
        else:
            self.warnings.append(f"Data directory provided does not exist: {data_dir}")
            self.init_status = False

        # File type
        
        self.proc_files = []
        self.file_types = {}
        if len(self.valid_files) > 0 and single_file_type:
            for file in self.valid_files:
                file_ending = self.get_filetype(file)
                if file_ending not in self.file_types.keys():
                    self.file_types[file_ending] = 1
                else:
                    self.file_types[file_ending] += 1
            
            self.file_types = sorted(self.file_types.items(), key=lambda x: x[1], reverse=True)
            self.file_type = self.file_types[0][0].lower()
            if file_type != "Auto":
                if file_type.lower() in [file_type_tmp[0].lower() for file_type_tmp in self.file_types]:
                    self.file_type = file_type
                else:
                    self.warnings.append(f"File type {file_type} not found in '{data_dir}', defaulted to Automatic Filetype Selection: {self.file_type}")
            for file in self.valid_files:
                file_ending = self.get_filetype(file)
                if file_ending == self.file_type:
                    self.proc_files.append(file)
                else:
                    self.warnings.append(f"File '{file}' excluded due to bad file type")
        
        elif len(self.valid_files) > 0 and not single_file_type:
            for file in self.valid_files:
                file_ending = self.get_filetype(file)
                if file_ending in multi_filetypes:
                    self.proc_files.append(file)
                else:
                    self.warnings.append(f"File '{file}' excluded due to bad file type")



        ## Metadata
                    
        if self.metadata:
            if os.path.exists(metadata_file):
                file_type = self.get_filetype(metadata_file)
                if file_type == ".csv":
                    self.metadata_df = pd.read_csv(metadata_file)
                elif file_type == '.xlsx':
                    self.metadata_df = pd.read_excel(metadata_file)
                elif file_type == ".tsv":
                    self.metadata_df = pd.read_csv(metadata_file, sep='\t')
                else:
                    self.warnings.append(f"Metadata file is unknown file type '{file_type}'")
                    self.init_status = False
            else:
                self.warnings.append(f"Metadata file '{metadata_file}' does not exist")
                self.init_status = False

            if self.init_status:
                self.metadata_attrs = self.metadata_df.columns
                if metadata_id_column not in self.metadata_attrs:
                    self.warnings.append(f"Metadata ID column '{metadata_id_column}' not in metadata")
                    self.init_status = False
                else:
                    self.metadata_df[metadata_id_column] = self.metadata_df[metadata_id_column].apply(lambda x: x.strip().lower())
                    for file in self.proc_files:
                        file_w_ending = file.split(os.sep)[-1].lower()
                        file_wo_ending = file.split(os.sep)[-1].lower().replace(self.get_filetype(file), "")
                        if file_w_ending in self.metadata_df[metadata_id_column].values or file_wo_ending in self.metadata_df[metadata_id_column].values:
                            pass
                        else:
                            self.warnings.append(f"File '{file}' cannot be located in metadata file")      
                
        ## Attributes
        full_attributes = ['lemma', 'pos', 'tag', 'dep', 'shape', 'is_alpha', 'is_stop', 'pymusas']
        bad_attributes = [attr for attr in attributes if attr not in full_attributes]
        if len(bad_attributes) != 0:
            self.warnings.append(f"Invalid attributes not used: {', '.join(bad_attributes)}")

        ## Multi-Process
        if multi_process:
            if not isinstance(worker_nodes, int):
                self.warnings.append(f"Invalid multiprocess parameters '{worker_nodes}', default to standard process")
                worker_nodes = 1
            CPUs = os.cpu_count()
            if worker_nodes > CPUs:
                self.warnings.append(f"{worker_nodes} Worker nodes selected, warning this exceeds the CPU count of your PC ({CPUs} CPUs) and may cause notable performance issues")
            if worker_nodes < 1:
                self.warnings.append(f"You have selected {worker_nodes} worker nodes. You need at least 1 worker node in order for a program to operate, a default of 1 has been selected.")
                worker_nodes = 1

        
        # Output Directory
        if isinstance(output_dir, str):
            if os.path.exists(output_dir):
                if not use_nonempty_output_folder:
                    for dirs, paths, files in os.walk(output_dir):
                        for file in files:
                            self.init_status = False
                            self.warnings.append(f"File present in output directory '{os.path.join(dirs, file)}', override using use_nonempty_output_folder paramter")

            elif create_output_folder:
                if os.sep not in output_dir:
                    os.mkdir(output_dir)
                else:
                    self.build_directory(output_dir)
            else:
                self.warnings.append(f"Output directory '{output_dir}' does not exist and pipeline does not have permissions to create")
                self.init_status = False
        else:
            self.warnings.append(f"Output directory '{output_dir}' is incorrect type '{type(output_dir)}', cannot use or create")
            self.init_status = False
    

        ## Print Init Warnings
        if len(self.warnings) > 0 and warnings:
            print("### Initialisation Warnings Start ###")
            for index in range(len(self.warnings)):
                print(f"{index+1}) - {self.warnings[index]}")
            print("### Initialisation Warnings End ###")
        elif len(self.warnings) > 0 and not warnings:
            print(f"### {len(self.warnings)} Initialisation Error(s) Suppressed ###")
        else:
            print("### Initialisation Successful, All Parameters Valid ###")

        if self.init_status:
            print("### Initialisation Successful ###")
        else:
            print("### Initialisation Failed - See Warnings for Details ###")

    def get_filetype(self, filename):
        return os.path.splitext(filename)[1].lower()

    def build_directory(self, directory):
        full_path = []
        for path in directory.split(os.sep):
            full_path.append(path)
            if os.path.exists(os.path.join(*full_path)):
                continue
            else:
                os.mkdir(os.path.join(*full_path))    


    def job_handler(self):
        if self.init_status == True:
            if self.worker_nodes == 1 or self.multi_process == False:
                for input_file in tqdm(self.proc_files):
                    self.process_file(input_file)
        else:
            print("### Initialisation Failed - See Warnings for Details ###")
    
    def process_file(self, input_file):
        file_type = self.get_filetype(input_file)
        is_xml = file_type == ".xml"
        if self.flat_output_dir:
            output_file = os.path.join(self.output_dir, input_file.split(os.sep)[-1].lower().replace(file_type, ".xml"))
        else:
            self.build_directory(os.path.join(self.output_dir, str(os.sep).join(input_file.split(os.sep)[1:-1])))
            output_file = os.path.join(self.output_dir, str(os.sep).join(input_file.split(os.sep)[1:]).lower()).replace(file_type, ".xml")

        if self.compress:
            output_file = output_file.replace(".xml", ".pkl")

        if self.skip_processed_files and os.path.exists(output_file):
            return None

        with open(input_file, errors=self.errors) as file_read:
            content = file_read.read()

        soup = self.build_xml(input_file, content, is_xml)
        self.latest_file = soup.prettify()

        if self.compress or self.start_benchmark:
            self.latest_file_comp = zlib.compress(self.latest_file.encode("UTF-8"))

        if self.compress:
            with open(output_file, "wb") as pkl_write:
                pkl.dump(self.latest_file_comp, pkl_write)
        else:
            with open(output_file, "w") as xml_write:
                xml_write.write(self.latest_file)


    def build_xml(self, file, file_content, is_xml):

        if is_xml:
                soup = bs4(file_content, features="xml")
        else:
            soup = bs4(features="xml")

        file_w_ending = file.split(os.sep)[-1].lower()
        file_wo_ending = file.split(os.sep)[-1].lower().replace(self.get_filetype(file), "")
        valid_ID = file_wo_ending
        metadata = None

        if self.metadata:
            for pot_ID in [file_w_ending, file_wo_ending]:
                metadata = self.metadata_df.loc[self.metadata_df['ID'] == pot_ID]
                valid_ID = pot_ID
                if not metadata.empty:
                    break

            if metadata.empty:
                metadata = dict(self.metadata_attrs, [np.nan]*len(self.metadata_attrs))
            else:
                metadata = metadata.to_dict(orient='records')[0]
            
            metadata["static_ID"] = (12-len(str(self.static_ID)))*"0" + str(self.static_ID)
            self.static_ID += 1
            metadata_tag = soup.find(self.xml_metadata_node)
            if metadata_tag == None:
                metadata_tag = soup.new_tag(self.xml_metadata_node)
                soup.append(metadata_tag)
            
            metadata_tag.attrs.update(metadata)

        text_tag = soup.find(self.xml_text_node)
        if text_tag == None:
            text_tag = soup.new_tag(self.xml_text_node)
            soup.append(text_tag)
        elif text_tag.text != "":
            file_content = text_tag.text

        soup = self.gen_spacy_features(soup, file_content, valid_ID)

        return soup
    
    def gen_spacy_features(self, soup, content, file_ID):
        output_doc = self.nlp(content)

        main_tag = soup.find(self.xml_text_node)
        for sentence in output_doc.sents:
            sentence_tag = soup.new_tag("s")
            for token in sentence:
                word_tag = soup.new_tag("w")
                word_tag.string = token.text
                attributes = {
                    'lemma': token.lemma_,
                    'pos': token.pos_,
                    'tag': token.tag_,
                    'dep': token.dep_,
                    'shape': token.shape_,
                    'is_alpha': token.is_alpha,
                    'is_stop': token.is_stop,
                    'pymusas': token._.pymusas_tags
                }
                for attribute in list(attributes.keys()):
                    if attribute not in self.attributes:
                        del attributes[attribute]
                word_tag.attrs.update(attributes)
                sentence_tag.append(word_tag)
            main_tag.append(sentence_tag)

        soup.append(main_tag)
        # Return the modified xml_soup
        return soup
    
    def benchmark(self, word_count=True):
        if self.init_status == False:
            print("### Initialisation Failed - See Warnings for Details ###")
            return None
        file_metadata = {}
        for file in tqdm(self.proc_files):
            file_type = self.get_filetype(file)
            with open(file, errors=self.errors) as read_file:
                content = read_file.read()
            if file_type == ".xml":
                content = bs4(content, features="xml").text
            word_count = len(content.split(" "))
            file_metadata[file] = word_count


        sorted_files = sorted(file_metadata.items(), key=lambda x: x[1])

        # Calculate the number of files to pick
        step_size = len(sorted_files) // self.benchmark_sample

        sample_performance = []
        sample_size_comp = []
        sample_size_raw = []
        # Pick files with the best distribution of scope
        selected_files = [file for i, (file, _) in enumerate(sorted_files) if i % step_size == 0][:self.benchmark_sample]
        for file in tqdm(selected_files):
            start = time.time()
            self.process_file(file)
            end = time.time()
            duration = end - start 
            sample_performance.append((file_metadata[file], duration))
            sample_size_raw.append((file_metadata[file], sys.getsizeof(self.latest_file)))
            sample_size_comp.append((file_metadata[file], sys.getsizeof(self.latest_file_comp)))

        seconds_runtime = sum(self.linear_estimator(sample_performance, file_metadata).values())
        bytes_raw = sum(self.linear_estimator(sample_size_raw, file_metadata).values())
        bytes_comp = sum(self.linear_estimator(sample_size_comp, file_metadata).values())

        print(f'Estimated Total Runtime = {humanize.naturaldelta(timedelta(seconds=seconds_runtime))}')
        print(f'Estimated Processed Corpus Size = {humanize.naturalsize(bytes_raw)} RAW or {humanize.naturalsize(bytes_comp)} Compressed')

    def linear_estimator(self, performance_list, file_metadata):
        X_train = np.array([data[0] for data in performance_list]).reshape(-1, 1)
        y_train = np.array([data[1] for data in performance_list])

        # Train a linear regression model
        model = LinearRegression().fit(X_train, y_train)

        # Use the model to predict runtimes for all files in the corpus
        predicted_values = {}
        for file, value in tqdm(file_metadata.items()):
            predicted_value = model.predict(np.array([[value]]))[0]
            predicted_values[file] = predicted_value

        return predicted_values
        


