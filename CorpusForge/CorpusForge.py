# CASS CorpusForge #
# Institution: Lancaster University #
# Author: Samuel Hollands & Hanna Schmueck #
# Contact: shollands1@sheffield.ac.uk & h.schmueck@lancaster.ac.uk #

import argparse
from CorpusForge.SpaCy_Pipeline_Class import spacyPipeline

def main():
    parser = argparse.ArgumentParser(description='Print parameters with -help option')

    # Add your parameters here
    parser.add_argument('--corpus_title', type=str, help='Title of the Corpus [DEFAULT: "SpaCy Pipeline Corpus"]')
    parser.add_argument('--file_type', type=str, help='File type of content files (.xml, .txt etc) [DEFAULT: "Auto"]')
    parser.add_argument('--multi_process', type=bool, help='Run multiple workers across cores [WIP] [DEFAULT: False]')
    parser.add_argument('--worker_nodes', type=int, help='Number of simultaneous worker nodes [DEFAULT: 1]')
    parser.add_argument('--attributes', type=list, help="List of SpaCy attributes to collect [DEFAULT: ['lemma', 'pos', 'tag', 'dep', 'shape', 'is_alpha', 'is_stop', 'pymusas']")
    parser.add_argument('--warnings', type=bool, help='Overtly print warnings [DEFAULT: True]')
    parser.add_argument('--metadata', type=bool, help='Embed metadata from a file into an XML header [DEFAULT: True]')
    parser.add_argument('--metadata_file', type=str, help='Metadata file path [DEFAULT: "metadata/metadata.csv"]')
    parser.add_argument('--metadata_ID_column', type=str, help='Name of ID column in metadata file [DEFAULT: "ID"]')
    parser.add_argument('--data_dir', type=str, help='Directory location of files to process [DEFAULT: "data"]')
    parser.add_argument('--all_files', type=list, help='Provide a list object of files for manual targeting of pipeline [DEFAULT: []]')
    parser.add_argument('--single_file_type', type=bool, help='Are all corpus files a single file type (excluding metadata file) [DEFAULT: True]')
    parser.add_argument('--multi_filetypes', type=list, help='List object containing all file extensions if single_file_type True [DEFAULT: []]')
    parser.add_argument('--output_dir', type=str, help='Directory location of files to output [DEFAULT: "output"]')
    parser.add_argument('--create_output_folder', type=bool, help='Create output directory if it does not already exist [DEFAULT: True]')
    parser.add_argument('--use_nonempty_output_folder', type=bool, help='Use an output folder even if it already contains files [DEFAULT: False]')
    parser.add_argument('--flat_output_dir', type=bool, help='Store all files in single layer directory rather than duplicating source directory structure [DEFAULT: False]')
    parser.add_argument('--xml_text_node', type=str, help='Node name for XML element storing textual content [DEFAULT: "text"]')
    parser.add_argument('--xml_metadata_node', type=str, help='Node name for XML element storing metadata content [DEFAULT: "text"]')
    parser.add_argument('--spacy_features', type=bool, help='Collect SpaCy features [DEFAULT: True]')
    parser.add_argument('--errors', type=str, help='File opening error parameter (bypass encoding errors) [DEFAULT: "strict""]')
    parser.add_argument('--start_benchmark', type=bool, help='Run a small scale benchmark to determine full corpus runtime [DEFAULT: False]')
    parser.add_argument('--benchmark_sample', type=int, help='Number of files in benchmark sample [DEFAULT: 20]')
    parser.add_argument('--compress', type=bool, help='Compress output into Pickle files [DEFAULT: False]')
    parser.add_argument('--skip_processed_files', type=bool, help='Processed files that already exist in the output_dir will be skipped [DEFAULT: False]')
    

    args = vars(parser.parse_args())
    filtered_args = {key: value for key, value in args.items() if value is not None}
    return filtered_args

if __name__ == '__main__':
    program_args = main()
    
    pipeline = spacyPipeline(**program_args)
    if pipeline.init_status == False:
        pass
    elif pipeline.start_benchmark == True:
        pipeline.benchmark()
    else:
        pipeline.job_handler()


