#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script: download_csye_for_mfa.py

Description:
    This script downloads and prepares the Corpus of Spoken Yiddish in Europe
    for use with the Montreal Forced Aligner.

    It performs the following tasks:
    1. Downloads and extracts TextGrid files from the CSYE-Transcripts repository.
    2. Downloads and extracts the bulk m4a audio archive, then downloads any missing audio files.
    3. Converts m4a files to WAV format for MFA compatibility.
    4. Modifies the transcript text so that angle brackets are wrapped around
       <individual> <words>, rather than <whole phrases>.
    5. Creates a basic MFA pronunciation dictionary and configuration file.

Author: Isaac L. Bleaman
Contact: bleaman@berkeley.edu
Created: 2024-09-10
Last Modified: 2025-04-14

License: CC BY-NC-SA 4.0

Usage:
    python download_csye_for_mfa.py [output_directory] [--transcript-set TRANSCRIPT_SET]

    If no output directory is specified, it defaults to 'mfa_workspace'.
    TRANSCRIPT_SET can be 'all', 'reviewed' (only), or 'unreviewed' (only). Default is 'reviewed'.

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
import sys
import time

AUDIO_FILES_URL = 'https://gist.githubusercontent.com/ibleaman/87217c5a30cb0376782126984c64197f/raw/CSYE-Audio.csv'
TRANSCRIPTS_ZIP_URL = 'https://github.com/YiddishCorpus/CSYE-Transcripts/archive/main.zip'
BULK_M4A_ZIP_URL = 'https://berkeley.box.com/shared/static/oc0ovb3m9w27g13trp990vmsc69jcmhr'
WORD_REGEX = re.compile(r"[\p{L}\-'<>]+")
FILLERS = ['spn', 'uh', 'ah', 'eh', 'oh', 'uhm', 'ehm', 'mhm', 'hm', 'mm', 'tsk']

def parse_arguments():
    parser = argparse.ArgumentParser(description="Download and prepare CSYE for Montreal Forced Aligner")
    parser.add_argument('output_directory', nargs='?', default='mfa_workspace', 
                        help="Directory to store all files (default: mfa_workspace)")
    parser.add_argument('--transcript-set', choices=['all', 'reviewed', 'unreviewed'], default='reviewed',
                        help="Choose which transcripts to use: 'all', 'reviewed', or 'unreviewed' (default: reviewed)")
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
                    
def copy_and_rename_textgrid_files(src_dir, dest_dir, transcript_set):
    """
    Copy and rename TextGrid files according to the specified transcript set.
    
    Args:
        src_dir: Base directory containing TextGrid folder
        dest_dir: Destination directory for copied files
        transcript_set: Which transcripts to include ('all', 'reviewed', or 'unreviewed')
        
    Returns:
        set: Set of tape IDs (base filenames without extension) that were processed
    """
    textgrid_base_dir = os.path.join(src_dir, 'TextGrid')
    
    if transcript_set == 'all':
        # Get files from both reviewed and unreviewed directories
        src_dirs = [
            os.path.join(textgrid_base_dir, 'reviewed'),
            os.path.join(textgrid_base_dir, 'unreviewed')
        ]
        print(f"Copying and renaming TextGrid files from both 'reviewed' and 'unreviewed' folders")
    else:
        # Get files from either reviewed or unreviewed directory
        src_dirs = [os.path.join(textgrid_base_dir, transcript_set)]
        print(f"Copying and renaming TextGrid files from '{transcript_set}' folder only")
    
    os.makedirs(dest_dir, exist_ok=True)
    
    # Track which files we've already copied (to handle potential duplicates)
    processed_files = set()
    
    for src_dir in src_dirs:
        if not os.path.exists(src_dir):
            print(f"Warning: Directory {src_dir} does not exist. Skipping.")
            continue
            
        for filename in os.listdir(src_dir):
            if filename.endswith(".la.TextGrid"):
                base_name = filename.replace(".la.TextGrid", "")
                
                # Skip if we've already processed this file (for 'all' option)
                if base_name in processed_files:
                    print(f"  Skipping duplicate file: {filename}")
                    continue
                    
                src_path = os.path.join(src_dir, filename)
                new_filename = f"{base_name}.TextGrid"
                dest_path = os.path.join(dest_dir, new_filename)
                
                shutil.copy(src_path, dest_path)
                processed_files.add(base_name)
                print(f"  Copied: {filename} → {new_filename}")
    
    return processed_files

# Functions for audio downloading and conversion

def download_csv(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.text

def download_audio_file(audio_link, file_name):
    if os.path.exists(file_name):
        print(f"{file_name} already exists; skipping download.")
        return True
    else:
        print(f"Downloading {os.path.basename(file_name)}")
        try:
            response = requests.get(audio_link)
            response.raise_for_status()  # Raises an HTTPError for bad requests
            with open(file_name, 'wb') as file:
                file.write(response.content)
            return True
        except Exception as e:
            print(f"Error downloading {file_name}: {e}")
            return False

def download_bulk_m4a_zip(url, extract_to):
    """
    Download and extract the bulk m4a zip file.
    
    Args:
        url: URL to the m4a zip file
        extract_to: Directory to extract files to
        
    Returns:
        set: Set of downloaded m4a filenames (without extension)
    """
    print(f"Downloading bulk m4a archive from {url}")
    print("This may take some time depending on your internet connection...")
    
    # Use a temporary file for the ZIP download
    zip_file_path = os.path.join(extract_to, "CSYE-m4a.zip")
    
    # Check if the ZIP file has already been downloaded
    if os.path.exists(zip_file_path):
        print(f"ZIP file already exists at {zip_file_path}")
    else:
        try:
            print("Starting download (this may take several minutes)...")
            
            # Download with progress indication
            response = requests.get(url, stream=True)
            response.raise_for_status()
            
            # Get total size if available
            total_size = int(response.headers.get('content-length', 0))
            downloaded = 0
            
            with open(zip_file_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        file.write(chunk)
                        downloaded += len(chunk)
                        
                        # Print progress
                        if total_size > 0:
                            percent = int(100 * downloaded / total_size)
                            sys.stdout.write(f"\rDownload progress: {percent}% ({downloaded/1024/1024:.1f} MB / {total_size/1024/1024:.1f} MB)")
                            sys.stdout.flush()
            
            print("\nDownload complete!")
        
        except Exception as e:
            print(f"\nError downloading bulk m4a archive: {e}")
            if os.path.exists(zip_file_path):
                os.remove(zip_file_path)
            return set()
    
    # Extract the ZIP file
    print(f"Extracting ZIP file to {extract_to}...")
    available_files = set()
    
    try:
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            # Extract only the m4a files
            for member in zip_ref.namelist():
                if member.endswith('.m4a'):
                    # Get just the filename
                    filename = os.path.basename(member)
                    # Add to available files set (without extension)
                    available_files.add(os.path.splitext(filename)[0])
                    
                    # Extract the file
                    source = zip_ref.open(member)
                    target_path = os.path.join(extract_to, filename)
                    
                    # Skip if the file already exists
                    if os.path.exists(target_path):
                        source.close()
                        continue
                    
                    with open(target_path, 'wb') as target:
                        target.write(source.read())
                    source.close()
        
        print(f"Extracted {len(available_files)} m4a files")
        
        # Optionally remove the ZIP file to save space
        # os.remove(zip_file_path)
        
    except Exception as e:
        print(f"Error extracting ZIP file: {e}")
    
    return available_files

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

def process_audio_files(csv_content, m4a_dir, corpus_dir, selected_tapes=None):
    """
    Process audio files: first use bulk archive, then download missing files.
    
    Args:
        csv_content: CSV content with audio links
        m4a_dir: Directory to store m4a files
        corpus_dir: Directory to store wav files
        selected_tapes: Set of tape IDs to process. If None, process all tapes.
    """
    os.makedirs(m4a_dir, exist_ok=True)
    
    # First, try to download and extract the bulk m4a archive
    print("Step 1: Downloading and extracting bulk m4a archive...")
    available_from_archive = download_bulk_m4a_zip(BULK_M4A_ZIP_URL, m4a_dir)
    
    # Now process the CSV to identify and download any missing files
    print("Step 2: Checking for missing audio files...")
    
    # Count for reporting
    total_tapes = 0
    selected_count = 0
    missing_count = 0
    missing_downloads = 0
    
    # Parse the CSV
    csv_reader = csv.DictReader(csv_content.splitlines())
    
    # First, collect all the information
    tapes_info = []
    for row in csv_reader:
        audio_link = row['AudioLink']
        tape = row['Tape']
        tapes_info.append((tape, audio_link))
        total_tapes += 1
    
    # Now process the tapes
    for tape, audio_link in tapes_info:
        # Skip if we're filtering and this tape isn't in our selected list
        if selected_tapes is not None and tape not in selected_tapes:
            continue
            
        selected_count += 1
        m4a_file = os.path.join(m4a_dir, f"{tape}.m4a")
        wav_file = os.path.join(corpus_dir, f"{tape}.wav")
        
        # Check if the m4a file already exists in the archive
        if tape in available_from_archive or os.path.exists(m4a_file):
            # If it exists, just convert it to WAV
            convert_to_wav(m4a_file, wav_file)
        else:
            # If not, try to download it
            missing_count += 1
            success = download_audio_file(audio_link, m4a_file)
            if success:
                missing_downloads += 1
                convert_to_wav(m4a_file, wav_file)
    
    # Report summary
    print("\nAudio processing summary:")
    if selected_tapes is not None:
        print(f"- Selected {selected_count} of {total_tapes} audio files based on transcripts")
    print(f"- Used {len(available_from_archive)} files from the bulk archive")
    print(f"- Downloaded {missing_downloads} of {missing_count} missing files individually")

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
    pronunciation = re.sub(r'(?<=[אַעייִאָווּײײַױ])נ(?=[גכק])', 'ŋ', pronunciation)
    pronunciation = re.sub(r'(?<![אַעייִאָווּײײַױ])נ(?=[בגדהװזטכלמנספּפֿצקרש])', 'ń', pronunciation)
    pronunciation = re.sub(r'(?<![אַעייִאָווּײײַױ])ן', 'ń', pronunciation)
    pronunciation = re.sub(r'(?<![אַעייִאָווּײײַױ])ל(?=[בגדהװזטכלמנספּפֿצקרש]|$)', 'ł', pronunciation)
    pronunciation = re.sub('י', 'j', pronunciation)
    pronunciation = re.sub(r'j(?![אַעייִאָוײײַױ])', 'i', pronunciation)
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

def main(output_directory, transcript_set):
    corpus_dir = os.path.join(output_directory, 'csye')
    m4a_dir = os.path.join(output_directory, 'm4a')
    transcripts_dir = os.path.join(output_directory, 'CSYE-Transcripts')
    output_dict_file = os.path.join(output_directory, 'csye_pronunciation_dict.txt')
    output_config_file = os.path.join(output_directory, 'csye_config.yaml')

    # Clear only TextGrid files from the corpus directory
    if os.path.exists(corpus_dir):
        print(f"Clearing existing TextGrid files from corpus directory: {corpus_dir}")
        for filename in os.listdir(corpus_dir):
            if filename.endswith('.TextGrid'):
                file_path = os.path.join(corpus_dir, filename)
                os.remove(file_path)
    else:
        os.makedirs(corpus_dir, exist_ok=True)

    # Step 1: TextGrid processing
    print(f"Starting TextGrid processing (using {transcript_set} transcripts)...")
    download_and_extract_zip(TRANSCRIPTS_ZIP_URL, transcripts_dir)
    selected_tapes = copy_and_rename_textgrid_files(transcripts_dir, corpus_dir, transcript_set)
    
    # Step 2: Audio processing - now with bulk download first
    print(f"Starting audio processing for {len(selected_tapes)} selected tapes...")
    csv_content = download_csv(AUDIO_FILES_URL)
    process_audio_files(csv_content, m4a_dir, corpus_dir, selected_tapes)

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
    print(f"Transcript set used: {transcript_set}")
    print(f"Total tapes processed: {len(selected_tapes)}")

if __name__ == '__main__':
    args = parse_arguments()
    main(args.output_directory, args.transcript_set)