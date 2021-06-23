from datetime import datetime
import json
import numpy as np
from matplotlib.figure import Figure
import os, sys
import matplotlib.patches as mpatches
from pathlib import Path

METRICS_DIR = "./metrics/"
GRAPHS_DIR = sys.argv[1]

def process_metrics(metrics_file):
    metrics = json.load(open(metrics_file, "r"))

    for institution in metrics:
        if institution not in complete_metrics:
            complete_metrics[institution] = {"actual": [], "numfound": []}
        complete_metrics[institution]["actual"].append(metrics[institution]["actual"])
        complete_metrics[institution]["numfound"].append(metrics[institution]["numfound"])


def get_long_color(c):
    if 'b' in c:
        return "blue"
    elif 'g' in c:
        return "green"
    elif 'r' in c:
        return "red"
    elif 'c' in c:
        return "cyan"
    elif 'm' in c:
        return "magenta"
    elif 'y' in c:
        return "yellow"
    else:
        return None


def graph(length):
    now = datetime.now()
    dt = str(now.strftime("%Y%m%d"))
    time = [i for i in range(1, length + 1)]
    colors = ['b-', 'g-', 'r-', 'c-', 'm-', 'y-']
    c = 0
    handles = []
    n = 0
    fig = Figure()
    ax = fig.add_subplot(1, 1, 1)
    for institution in complete_metrics:
        a = np.array(complete_metrics[institution]["actual"])
        nf = np.array(complete_metrics[institution]["numfound"])
        try:
            ax.plot(time, a, colors[c])
            alc = get_long_color(colors[c])
            c += 1
            ax.plot(time, nf, colors[c])
            nlc = get_long_color(colors[c])
            c += 1
        except:
            continue
        actual = mpatches.Patch(color=alc, label=institution+'-actual')
        numf = mpatches.Patch(color=nlc, label=institution+'-numfound')
        handles.append(actual)
        handles.append(numf)
        ax.set_xlabel('Run Number')
        ax.set_title("CIC Metrics")
        if len(handles) == 6 or institution == list(complete_metrics.keys())[-1]:
            n += 1
            ax.legend(handles=handles)
            Path(GRAPHS_DIR + '/graph-' + dt + '.png').touch()
            fig.savefig(GRAPHS_DIR + '/graph-' + dt + '-' + str(n) + '.png')
            handles = []
            c = 0
            fig = Figure()
            ax = fig.add_subplot(1, 1, 1)


if __name__ == "__main__":

    complete_metrics = {}

    metrics_files = sorted(Path(METRICS_DIR).iterdir(), key=os.path.getmtime, reverse=True)[:30]

    for mf in metrics_files:
        process_metrics(str(mf))

    graph(len(metrics_files))
