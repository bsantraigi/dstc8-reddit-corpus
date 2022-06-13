#!/usr/bin/env python
# coding: utf-8

# # Steps
# 
# - Load a year
# - Read data
# - Apply reddit-filter (r1m_preprocess)
# - split into json files, each of 512 MB max
# - Remove the max_conv limiter
# - only extract in-domain validation data

import os, glob, re, json, random
from tqdm import tqdm
from r1m_preprocess import filter_dialogs
import argparse

def extract_text(conv):
    text = " __eou__ ".join(conv['turns']) + " __eou__"
    text = text.replace("\n", "")
    text = text.replace("\r", "")
    return text


def cmdline_args():
    # year
    parser = argparse.ArgumentParser()
    parser.add_argument("-y", "--year", type=str, default="2011", help="Year to be processed. Expects a directory data/<year>")

    args = parser.parse_args()
    return args


args = cmdline_args()
year = args.year
training_files = glob.glob(f'./data/{year}/dialogues/training/*.txt')


print(f"Processing {len(training_files)} training files")


MAX_CONV_PER_DOMAIN = float('inf')

train_data = []
for file in tqdm(training_files):
    with open(file) as f:
        for i, l in enumerate(f):
            if i >= MAX_CONV_PER_DOMAIN:
                break
            # conv = extract_text(json.loads(l))
            conv = json.loads(l)["turns"]
            train_data.append(conv)
        print(f"Loaded {i} samples from {file}")


print(f"Example: {train_data[0]}")
print(f"Loaded {len(train_data)} train-samples")


_, train_data = filter_dialogs(train_data)


val_files = glob.glob(f'./data/{year}/dialogues/validation_date_in_domain_in/*.txt')


print(f"Processing {len(val_files)} validation files")

val_data = []
for file in tqdm(val_files):
    with open(file) as f:        
        for j, l in enumerate(f):
            if j >= MAX_CONV_PER_DOMAIN:
                break
            # conv = extract_text(json.loads(l))  
            conv = json.loads(l)["turns"]
            val_data.append(conv)
        print(f"Loaded {j} samples from {file}")


print(f"Example: {val_data[0]}")

print(f"Loaded {len(val_data)} val-samples")

_, val_data = filter_dialogs(val_data)


random.seed(42)


random.shuffle(train_data)
random.shuffle(val_data)


test_data = val_data[1000:]
val_data = val_data[:1000]


print(f"Train: {len(train_data)}, Val: {len(val_data)}, Test: {len(test_data)}")


print(f"Example: {test_data[0]}")

exp_path = './data/reddit_xtreme/'
try:
    os.makedirs(exp_path)
    print(f"Path {exp_path} created.")
except FileExistsError:
    print(f"Path {exp_path} exists.")
    pass


class SplitWriter:
    def __init__(self, base_path_prefix, max_split_mb=512):
        """
        @param base_path_prefix: We will add an incremental id and .json at the end.
        """
        self.current_file = 0
        self.base_path_prefix = base_path_prefix
        self.f = None
        self.max_split_mb = max_split_mb
        
        self._open_next()
    
    def _open_next(self):
        if self.f is not None:
            self.f.close()
        
        next_file = f"{self.base_path_prefix}_{self.current_file:02d}.json"
        print("Opening next file:", next_file)
        self.f = open(next_file, "w")
        self.current_file += 1
        
    def write(self, l):        
        self.f.write(l)
        
        if self.f.tell() > self.max_split_mb*1024*1024:
            print("Reached Max Size")
            self._open_next()
        
    def close(self):
        if self.f is not None:
            self.f.close()


# Train
train_writer = SplitWriter(os.path.join(exp_path, f'train_{year}'), max_split_mb=512)

for l in train_data:
    train_writer.write(json.dumps({"turns": l})+"\n")
    
train_writer.close()

# Valid
valid_writer = SplitWriter(os.path.join(exp_path, f'valid_{year}'), max_split_mb=64)

for l in val_data:
    valid_writer.write(json.dumps({"turns": l})+"\n")

valid_writer.close()

# Test
test_writer = SplitWriter(os.path.join(exp_path, f'test_{year}'), max_split_mb=512)

for l in test_data:
    test_writer.write(json.dumps({"turns": l})+"\n")
    
test_writer.close()







