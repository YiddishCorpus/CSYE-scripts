#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script: download_csye_for_mfa.py

Description:
    This script downloads and prepares the Corpus of Spoken Yiddish in Europe
    for use with the Montreal Forced Aligner.

    It performs the following tasks:
    1. Downloads and extracts TextGrid files from the CSYE-Transcripts repository.
    2. Downloads the corresponding audio files and converts them to WAV format.
    3. Modifies the transcript text so that angle brackets are wrapped around
       <individual> <words>, rather than <whole phrases>.
    4. Creates a basic MFA pronunciation dictionary and configuration file.

Author: Isaac L. Bleaman
Contact: bleaman@berkeley.edu
Created: 2024-09-10
Last Modified: 2024-09-16

License: CC BY-NC-SA 4.0

Usage:
    python download_csye_for_mfa.py [output_directory]

    If no output directory is specified, it defaults to 'mfa_workspace'.

Notes:
    - Ensure that ffmpeg is installed and accessible from the command line.
    - Internet connection is required to download the necessary files.
    - The final MFA-compatible corpus will be available in the
      '[output_directory]/csye' directory.
"""

import os
import requests
import csv
import subprocess
import zipfile
from io import BytesIO
import shutil
import argparse
import regex as re
from praatio import textgrid
from praatio.utilities.constants import Interval
import yiddish

AUDIO_FILES_URL = 'https://gist.githubusercontent.com/ibleaman/87217c5a30cb0376782126984c64197f/raw/CSYE-Audio.csv'
TRANSCRIPTS_ZIP_URL = 'https://github.com/YiddishCorpus/CSYE-Transcripts/archive/main.zip'
WORD_REGEX = re.compile(r"[\p{L}\-'<>]+")
FILLERS = ['spn', 'uh', 'ah', 'eh', 'oh', 'uhm', 'ehm', 'mhm', 'hm', 'mm', 'tsk']

def parse_arguments():
    parser = argparse.ArgumentParser(description="Download and prepare CSYE for Montreal Forced Aligner")
    parser.add_argument('output_directory', nargs='?', default='mfa_workspace', 
                        help="Directory to store all files (default: mfa_workspace)")
    return parser.parse_args()

# Functions for TextGrid downloading and organizing

def download_and_extract_zip(url, extract_to):
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Failed to download file, status code {response.status_code}")
    
    zip_file_stream = BytesIO(response.content)
    
    with zipfile.ZipFile(zip_file_stream, 'r') as zip_ref:
        # Zip contains a root folder we want to ignore
        top_level_dir = next((member.split('/')[0] for member in zip_ref.namelist()), None)
        
        # Iterate through all members (files and directories) in the zip
        for member in zip_ref.namelist():
            # Skip directories, we only want to extract files
            if not member.endswith('/'):
                new_path = os.path.join(extract_to, os.path.relpath(member, top_level_dir))
                os.makedirs(os.path.dirname(new_path), exist_ok=True)
                with zip_ref.open(member) as source, open(new_path, 'wb') as target:
                    target.write(source.read())
                    
def copy_and_rename_textgrid_files(src_dir, dest_dir):
    print(f"Copying and renaming TextGrid files (in Latin orthography) from {src_dir} to {dest_dir}")
    os.makedirs(dest_dir, exist_ok=True)
    
    for filename in os.listdir(src_dir):
        if filename.endswith(".la.TextGrid"):
            src_path = os.path.join(src_dir, filename)
            new_filename = filename.replace(".la", "")
            dest_path = os.path.join(dest_dir, new_filename)
            shutil.copy(src_path, dest_path)

# Functions for audio downloading and conversion

def download_csv(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.text

def download_audio_file(audio_link, file_name):
    if os.path.exists(file_name):
        print(f"{file_name} already exists; skipping download.")
    else:
        print(f"Downloading {os.path.basename(file_name)}")
        response = requests.get(audio_link)
        response.raise_for_status()  # Raises an HTTPError for bad requests
        with open(file_name, 'wb') as file:
            file.write(response.content)

def convert_to_wav(m4a_file, wav_file):
    if os.path.exists(wav_file):
        print(f"     {os.path.basename(wav_file)} already exists; skipping conversion.")
    else:
        print(f"     Converting {os.path.basename(m4a_file)} to .wav")
        subprocess.run([
            'ffmpeg',
            '-i', m4a_file,
            '-ar', '44100',
            wav_file,
            '-loglevel', 'quiet'
        ], check=True)

def process_csv_and_download(csv_content, m4a_dir, corpus_dir):
    csv_reader = csv.DictReader(csv_content.splitlines())
    for row in csv_reader:
        audio_link = row['AudioLink']
        tape = row['Tape']
        m4a_file = os.path.join(m4a_dir, f"{tape}.m4a")
        wav_file = os.path.join(corpus_dir, f"{tape}.wav")
        
        download_audio_file(audio_link, m4a_file)
        convert_to_wav(m4a_file, wav_file)

# Functions for fixing brackets in transcripts

def wrap_words_in_brackets(text):
    if text is None or text.strip() == "":
        return ""

    def replace_bracketed(match):
        content = match.group(1).strip()
        words_and_punctuation = re.findall(r"[\p{L}\-']+|[^\p{L}\s<>]+", content)
        
        formatted_content = []
        for i, item in enumerate(words_and_punctuation):
            if re.match(r"[\p{L}\-']+", item):
                formatted_content.append(f'<{item}>')
            else:
                if formatted_content:
                    formatted_content[-1] += item
                else:
                    formatted_content.append(item)
        
        return ' '.join(formatted_content)

    return re.sub(r'<([^>]+)>', replace_bracketed, text)

def process_textgrid_file(file_path):
    tg = textgrid.openTextgrid(file_path, includeEmptyIntervals=True)
    
    for tier in tg.tiers:
        new_entries = []
        for entry in tier.entries:
            new_label = wrap_words_in_brackets(entry.label)
            new_entry = Interval(entry.start, entry.end, new_label)
            new_entries.append(new_entry)
        
        new_tier = textgrid.IntervalTier(tier.name, new_entries, minT=tier.minTimestamp, maxT=tier.maxTimestamp)
        tg.replaceTier(tier.name, new_tier)
    
    tg.save(file_path, format="long_textgrid", includeBlankSpaces=True)
    print(f"Processed and overwrote: {os.path.basename(file_path)}")

# Functions for creating pronunciation dictionary from all words in transcripts

def yiddish_to_pronunciation(word):
    pronunciation = word
    pronunciation = re.sub('זש', 'ʒ', pronunciation)
    pronunciation = re.sub('טש', 'ʧ', pronunciation)
    pronunciation = re.sub(r'(?<=[אַעייִאָווּײײַױ])נ(?=[גכק])', 'ŋ', pronunciation)
    pronunciation = re.sub(r'(?<![אַעייִאָווּײײַױ])נ(?=[בגדהװזטכלמנספּפֿצקרש])', 'ń', pronunciation)
    pronunciation = re.sub(r'(?<![אַעייִאָווּײײַױ])ן', 'ń', pronunciation)
    pronunciation = re.sub(r'(?<![אַעייִאָווּײײַױ])ל(?=[בגדהװזטכלמנספּפֿצקרש]|$)', 'ł', pronunciation)
    pronunciation = re.sub('י', 'j', pronunciation)
    pronunciation = re.sub(r'j(?![אַעייִאָוײײַױ])', 'i', pronunciation)
    pronunciation = re.sub('j', 'y', pronunciation)

    pronunciation = ' '.join([yiddish.transliterate(c) for c in pronunciation])
    pronunciation = re.sub('ʒ', 'zh', pronunciation)
    pronunciation = re.sub('ʧ', 'tsh', pronunciation)
    pronunciation = re.sub('ŋ', 'ng', pronunciation)
    pronunciation = re.sub('ń', 'en', pronunciation)
    pronunciation = re.sub('ł', 'el', pronunciation)
    pronunciation = re.sub('❓', 'TBD', pronunciation)

    pronunciation = re.sub('[^\w ]', '', pronunciation)
    pronunciation = re.sub(r' +', ' ', pronunciation)
    pronunciation = pronunciation.strip()

    return pronunciation

def create_pronunciation_dictionary(corpus_dir, output_dict_file):
    unique_words = set()

    for filename in os.listdir(corpus_dir):
        if filename.endswith(".TextGrid"):
            filepath = os.path.join(corpus_dir, filename)
            tg = textgrid.openTextgrid(filepath, includeEmptyIntervals=True)

            for tier_name in tg.tierNames:
                tier = tg.getTier(tier_name)
                if isinstance(tier, textgrid.IntervalTier):
                    for _, _, label in tier.entries:
                        if label:
                            words = WORD_REGEX.findall(label)
                            unique_words.update(words)

    sorted_words = sorted(unique_words)
    pronunciation_dictionary = {}

    for word in sorted_words:
        if word not in pronunciation_dictionary:
            pronunciation_dictionary[word] = ''
        
        # We'll skip if word is ALLCAPS, geMIXt, or a filler
        if not re.search(r'[a-z]', word) or re.search('[A-Z]{2,}', word) or re.sub(r'[<>]', '', word) in FILLERS:
            pronunciation_dictionary[word] = 'TBD'
        else:
            detransliterated = yiddish.replace_punctuation(yiddish.detransliterate(word.replace("UNK", "❓").lower(), loshn_koydesh=False))
            phonemes = yiddish_to_pronunciation(detransliterated).split()
            pronunciation_dictionary[word] = ' '.join(phonemes)

    dictionary_string = '\n'.join(f'{word}\t{pronunciation_dictionary[word]}' for word in sorted(pronunciation_dictionary.keys()) if pronunciation_dictionary[word] != 'TBD')

    with open(output_dict_file, 'w') as f:
        f.write(dictionary_string)

    print(f"\tPronunciation saved to {output_dict_file}")

# Main function

def main(output_directory):
    corpus_dir = os.path.join(output_directory, 'csye')
    m4a_dir = os.path.join(output_directory, 'm4a')
    transcripts_dir = os.path.join(output_directory, 'CSYE-Transcripts')
    output_dict_file = os.path.join(output_directory, 'csye_pronunciation_dict.txt')
    output_config_file = os.path.join(output_directory, 'csye_config.yaml')

    # Step 1: TextGrid processing
    print("Starting TextGrid processing...")
    os.makedirs(corpus_dir, exist_ok=True)
    download_and_extract_zip(TRANSCRIPTS_ZIP_URL, transcripts_dir)
    copy_and_rename_textgrid_files(os.path.join(transcripts_dir, 'TextGrid'), corpus_dir)
    
    # Step 2: Audio processing
    print("Starting audio processing...")
    os.makedirs(m4a_dir, exist_ok=True)
    csv_content = download_csv(AUDIO_FILES_URL)
    process_csv_and_download(csv_content, m4a_dir, corpus_dir)

    # Step 3: Fix brackets in transcripts
    print("Fixing brackets in transcripts...")
    for filename in os.listdir(corpus_dir):
        if filename.endswith('.TextGrid'):
            file_path = os.path.join(corpus_dir, filename)
            process_textgrid_file(file_path)

    # Step 4: Create pronunciation dictionary and config file
    print("Creating pronunciation dictionary and config file...")
    create_pronunciation_dictionary(corpus_dir, output_dict_file)
    with open(output_config_file, 'w') as f:
        f.write('ignore_case: false\npunctuation: 、。।，@""(),.:;¿?¡!\&%#*~【】，…‥「」『』〝〟″⟨⟩♪・‹›«»～′$+=')
    print(f"\tMFA config file saved to {output_config_file}")

    print(f"Done! The corpus for the MFA can be found at: {corpus_dir}")

if __name__ == '__main__':
    args = parse_arguments()
    main(args.output_directory)