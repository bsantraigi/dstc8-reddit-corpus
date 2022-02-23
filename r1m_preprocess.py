import re
import os
import pandas as pd
import numpy as np
import tqdm
import argparse
# from transformers import BertTokenizer




def load_dialogs(path):
    dialogs = []
    with open(path) as f:
        for line in tqdm.tqdm(f, desc="Loading data"):
            # if len(self.data) > max_items:
            #     break  # Enough for now
            Full_D = line.strip().strip("__eou__").split(" __eou__ ")
            if len(Full_D) >= 2:
                dialogs.append(Full_D)
#                 for j in range(2, len(Full_D) + 1):
#                     D = Full_D[:j]
#                     C = " __eou__ ".join(D[:-1]).strip()
#                     R = D[-1].strip()
                    # mid = len(D)//2
                    # C = " __eou__ ".join(D[:mid])
                    # R = " __eou__ ".join(D[mid:])
    return dialogs


def filter_dialogs(dialog_arr):
    ext1 = r"io|com|org|net|us|co|biz|info|jobs|mobi|name|ly|tel|kitchen|email|tech|estate|xyz|codes|bargains|bid|expert|int|mil|edu|gov|ca|cn|fr|ch|au|in|de|jp|nl|uk|mx|no|ru|br|se|es"
    extensions = r"gt|cy|kg|jobs|coop|ag|li|pa|qa|uz|ly|al|so|mn|cr|dz|md|bz|travel|im|la|ke|lu|ba|do|uy|name|mk|ve|xyz|club|is|eg|bd|cat|ec|pw|am|nu|ma|ge|sa|asia|lk|to|fm|kz|xxx|country|pro|tk|ng|ee|th|si|xn--p1ai|ws|mobi|ph|lv|ae|rs|hr|pe|az|su|bg|gov|pk|lt|by|my|ie|nz|sg|hk|io|cl|il|pt|cc|fi|sk|id|no|dk|be|us|ar|at|ch|hu|me|biz|edu|se|vn|ro|mx|tr|tv|za|eu|co|cz|ua|tw|kr|ca|gr|es|nl|au|ir|info|cn|fr|it|pl|in|uk|br|jp|de|org|ru|net|com"

    url_reg = re.compile(r"([a-zA-Z0-9]+://)?([\w]{2,}\.){1,}(" + ext1 + r"|" + extensions + r")([/?]([^ \n()\'\"]+))?")

    word_reg = re.compile(r"[ ]+")
    # url_reg = re.compile(r"([a-zA-Z0-9]+://)?([\w]{2,}\.){1,}(io|com|org|net|us|co|biz|info|jobs|mobi|name|ly|tel|kitchen|email|tech|estate|xyz|codes|bargains|bid|expert|int|mil|edu|gov|ca|cn|fr|ch|au|in|de|jp|nl|uk|mx|no|ru|br|se|es)([/?]([^ \n()\'\"]+))?")
    post_title_regex = re.compile(r"<selfbr>")

    stats = []
    filtered_data = []
    filtered_data_array_format = []
    urls = []
    for d_raw in tqdm.tqdm(dialog_arr, desc="Filtering dialogs"):
        if len(d_raw) < 2:
            continue
        d = " __eou__ ".join(d_raw)

        # selfbr removal (comes after title of the post)
        d = post_title_regex.sub(":", d)
        
        wd = word_reg.split(d)
        
        # first utterance
        first_utt_len = len(word_reg.split(d_raw[0]))
        
        # url filter
        L = list(url_reg.finditer(d))
        d = url_reg.sub("[URL]", d)
        if L is not None:
            urls.extend([match.group() for match in L])

        # length filter
        ll_utt = [1 for u in d.split(" __eou__ ") if len(u.split(" ")) > 80]
        avg_utt_len = np.mean([len(u.split(" ")) for u in d.split(" __eou__ ")])
        
        # if avg_utt_len > 60:
        #     print(d.replace("__eou__", "\n\n>>\t"))
        #     print("=====================================\n")
        this_stat = {
            'urls': len(L),
            'avg_utt_len': avg_utt_len,
            'very_long_utt': len(ll_utt),
            'turns': len(d.split("__eou__")),
            'first_utt_len': first_utt_len
        }
        stats.append(this_stat)
        
        if this_stat['urls'] < 3 and this_stat['avg_utt_len'] <= 50 and \
                this_stat['avg_utt_len'] > 3 and this_stat['first_utt_len'] > 4 and \
                this_stat['very_long_utt'] == 0:
            filtered_data.append(d + " __eou__")
            filtered_data_array_format.append(d.split(" __eou__ "))


    print(f"salvaged {len(filtered_data)} dialogs out of a total {len(dialog_arr)} samples.")

    df_stats = pd.DataFrame(stats)
    print(df_stats.describe())
    
    return filtered_data, filtered_data_array_format

# df_stats.loc[df_stats.avg_utt_len > 50].shape
# df_stats.loc[df_stats.avg_utt_len < 4].shape
# df_stats.loc[df_stats.urls > 2].shape
# df_stats.loc[df_stats.very_long_utt > 0].shape
# df_stats.loc[df_stats.first_utt_len < 4].shape

def cmdline():
    p = argparse.ArgumentParser()
    p.add_argument("-d", "--data_dir", required=True, help="Folder where the .txt data file is located. (eg. ./data/reddit_1M/)")
    p.add_argument("-f", "--file_name", required=True, help="txt filename without the path (e.g. train_dialogs.txt)")
    return p.parse_args()


if __name__ == "__main__":
    args = cmdline()
    # tokenizer = BertTokenizer.from_pretrained("bert-base-uncased")
    # tokenizer.add_special_tokens({'sep_token': '__eou__'})

    # data_dir = "./data/reddit_1M/"
    data_dir = args.data_dir
    # use glob.glob to search
    # data_file = "train_dialogues.txt"
    data_file = args.file_name

    fx = os.path.join(data_dir, data_file)
    print(f"Loading {fx}")
    train_dialogs = load_dialogs(fx)
    # train_dialogs = load_dialogs("./data/dailydialog/dialogues_train.txt")
    print(train_dialogs[0])
    filtered_data, fdaf = filter_dialogs(train_dialogs)
    print(filtered_data[0])
    print(fdaf[0])
    try:
        os.makedirs(os.path.join(data_dir, "filtered/"))
    except FileExistsError:
        pass

    with open(os.path.join(data_dir, "filtered/", data_file), "w") as outf:
        for line in filtered_data:
            outf.write(line + "\n")

"""
python r1m-process.py -d ./data/reddit_1M/ -f test_dialogues.txt
python r1m-process.py -d ./data/reddit_1M/ -f val_dialogues.txt
python r1m-process.py -d ./data/reddit_1M/ -f train_dialogues.txt
"""
