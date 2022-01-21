import re
from pathlib import Path
import sys
import warnings

import pandas as pd

warnings.filterwarnings('ignore')


# path to directory contains experiment results
result_path = sys.argv[1]


def _path_2_comb(comb_dir_name):
    comb = comb_dir_name.replace('/', ' ').strip()
    i = iter(comb.split('-'))
    comb = dict(zip(i, i))
    comb['dirname'] = comb_dir_name
    return comb


def parse(path):
    metadata = dict()
    operations = list()
    with open(path, 'r') as f:
        for line in f:
            if 'Filebench Version' in line:
                metadata['filebench_version'] = line.split('Filebench Version')[1].strip()
            elif 'shared memory' in line:
                metadata['shared_mem'] = line.split('of shared memory')[0].split('Allocated')[1].strip()
            elif 'successfully loaded' in line:
                metadata['workload'] = line.split('personality successfully loaded')[0].split(':')[1].strip()
            elif 'populated' in line:
                metadata['dir_name'] = re.findall(': ?([\w|\.|\-]+) populated', line)[0]
                metadata['populated_files'] = int(re.findall(' ?populated: ?(\d+) file', line)[0])
                metadata['avg_dir_width'] = float(re.findall(' ?width ?= ?(\d+)', line)[0])
                metadata['avg_dir_depth'] = float(re.findall(' ?depth ?= ?(\d*\.?\d*)', line)[0])
                metadata['leafdirs'] = int(re.findall('(\d*\.?\d*) leafdirs', line)[0])
                metadata['total_size'] = re.findall('(\d*\.?\d*\w+) total size', line)[0]
            elif 'Run took' in line:
                metadata['run_duration'] = int(re.findall(' ?Run took ?(\d+) second', line)[0])
            else:
                tokens = [tok.strip() for tok in line.split(" ") if tok.strip()]
                if 'ops' in line and 'ms' in line:
                    if 'Summary' not in line:
                        operations.append({
                            'name': tokens[0],
                            'ops': int(tokens[1].split('ops')[0].strip()),
                            'ops_per_sec': float(tokens[2].split('ops')[0].strip()),
                            'mb_per_sec': float(tokens[3].split('mb')[0].strip()),
                            'latency': float(tokens[4].split('ms')[0].strip()),
                            'range_lower': float(re.findall('(\d*\.?\d*)ms', tokens[5])[0]),
                            'range_upper': float(re.findall('(\d*\.?\d*)ms', tokens[7])[0])
                        })
                    else:
                        operations.append({
                            'name': 'summary',
                            'ops': int(tokens[3].strip()),
                            'ops_per_sec': float(tokens[5].strip()),
                            'mb_per_sec': float(tokens[9].split('mb')[0].strip()),
                            'latency': float(tokens[10].split('ms')[0].strip()),
                        })
    return metadata, pd.DataFrame(operations)


def process_dir(dirpath):
    metadatas = list()
    dfs = list()
    for path in dirpath.glob('filebench*'):
        metadata, df_operations = parse(path)
        metadata['result_filename'] = path.name
        metadatas.append(metadata)
        dfs.append(df_operations)
    
    df_meta = pd.DataFrame(metadatas)
    df_operations = pd.concat(dfs)
    operations = list()
    for index, group in df_operations.groupby('name'):
        operation = {
            'name': index,
            'ops': group['ops'].sum(),
            'ops_per_sec': group['ops_per_sec'].mean(),
            'mb_per_sec': group['mb_per_sec'].mean(),
            'latency': group['latency'].mean(),
        }
        if index != 'summary':
            operation['range_lower'] = group['range_lower'].min()
            operation['range_upper'] = group['range_upper'].max()
        operations.append(operation)
    return df_meta, pd.DataFrame(operations)


p = Path(result_path)
df = pd.DataFrame([_path_2_comb(dirpath.name) for dirpath in p.iterdir()])
df.dropna(subset=['iteration'], inplace=True)
for col in df.columns:
    try:
        df = df.astype({col: int})
    except ValueError:
        print(col)
        continue

df.replace({'n_client': {100: 16, 1: 32, 2: 64, 3: 96}}, inplace=True)
df.sort_values(by=['n_dc', 'n_client', 'iteration'])


data = list()
metas = list()
for index, row in df.iterrows():
    print(f'Working on {row["dirname"]}')
    try:
        dirpath = p / row['dirname']
        df_meta, df_operations = process_dir(dirpath)
        df_meta['dirpath'] = dirpath
        metas.append(df_meta)
        df_comb = pd.DataFrame([row.values] * len(df_operations), columns=row.index)
        df_operations = df_operations.merge(df_comb, left_index=True, right_index=True, suffixes=['', '_comb'])
        df_operations.to_csv(dirpath / 'final_combine.csv', index=False)
        data.append(df_operations)
    except Exception as e:
        print(f'--> Exception {e} on {row["dirname"]}')

df_final = pd.concat(data)
df_final.sort_values(['dirname', 'name'], inplace=True)
df_final.to_csv(p / 'result.csv', index=False)
print(f'\nResults: {df_final}')

