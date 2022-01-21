import math
import matplotlib.pyplot as plt
from pathlib import Path
import sys

import pandas as pd

plt.rcParams.update({'font.size': 30.0})

result_path = Path(sys.argv[1])

if len(sys.argv) >= 3:
    plot_by = sys.argv[2]
else:
    plot_by = 'n_gluster_per_dc'


df = pd.read_csv(result_path)
groupby_cols = ['n_dc', 'n_client', plot_by, 'name']
df = df.groupby(groupby_cols).mean().reset_index()

df = df[df['name'] == 'summary']
print(f'Plot data: {df}')


def plot(df, label, color, marker, linewidth=5, markersize=20, annotations=None, x_range=None, y_range=None):
    x_ticks_interval = 10
    x = df['ops_per_sec']
    y = df['latency']

    if x_range is None:
        min_x = round(math.floor(x.min() - 1000), -3)
        max_x = round(math.ceil(x.max() + 1000), -3)
    else:
        min_x, max_x = x_range

    if y_range is None:
        min_y = 0
        max_y = round(math.ceil(y.max() + 10), -3)
    else:
        min_y, max_y = y_range

    plt.plot(x, y,
             linewidth=linewidth,
             markersize=markersize,
             color=color,
             marker=marker,
             markeredgewidth=4,
             fillstyle='none',
             label=label)
    ax = plt.gca()
    ax.set_xticks(range(min_x, max_x + 10, x_ticks_interval))
    ax.set_yticks(range(min_y, max_y + 1, 100))
    if annotations:
        # ax = plt.gca()
        for i, text in enumerate(annotations):
            if i + 1 > len(x):
                break
            t = plt.text(x.iloc[i] + 2, y.iloc[i] + 0.25, text)
            # t.set_bbox(dict(facecolor='white', edgecolor='white'))
            # ax.annotate(text, (x.iloc[i] - 100, y.iloc[i] + 2))

# elmerfs
# x_range = [75_000, 300_000]
# y_range = [200, 700]

# gluster
x_range = [150, 200]
y_range = [250, 800]

plot(df[df[plot_by] == 3], "3 nodes", "goldenrod", "X",
        x_range=x_range, y_range=y_range)
plot(df[df[plot_by] == 6], "6 nodes", "mediumblue", "d",
        x_range=x_range, y_range=y_range)
plot(df[df[plot_by] == 9], "9 nodes", "forestgreen", "d",
        x_range=x_range, y_range=y_range)
plot(df[df[plot_by] == 12], "12 nodes", "red", "o",
        annotations=[16, 32, 64, 96], x_range=x_range, y_range=y_range)


plt.legend(loc='best')
plt.grid()
ax = plt.gca()
ax.set_xlabel("Throughput (ops/s)")
ax.set_ylabel("Latency (ms)")

fig = plt.gcf()
fig.set_size_inches(20.5, 12.5)
plt.tight_layout()
fig.savefig(result_path.parent / 'plot.png', format="png", dpi=300)
