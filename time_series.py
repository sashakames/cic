import matplotlib
import json
import numpy as np
from matplotlib.figure import Figure
import os
import matplotlib.patches as mpatches

METRICS_DIR = "./metrics"

def process_metrics(metrics_file):
    metrics = json.load(open(metrics_file, "r"))

    for institution in metrics:
        if institution not in complete_metrics:
            complete_metrics[institution] = {"actual": [], "numfound": []}
        complete_metrics[institution]["actual"].append(metrics[institution]["actual"])
        complete_metrics[institution]["numfound"].append(metrics[institution]["numfound"])


def graph(length):
    fig = Figure()
    ax = fig.add_subplot(1, 1, 1)
    time = [i for i in range(0, length)]
    colors = []
    c = 0
    for institution in complete_metrics:
        a = np.array(complete_metrics[institution]["actual"])
        nf = np.array(complete_metrics[institution]["numfound"])
        ax.plot(time, a, colors[c])
        c += 1
        ax.plot(time, nf, colors[c])
        c += 1
        if c >= (len(colors) - 1):
            c = 0
        lc = "blue"
        blue = mpatches.Patch(color=lc, label=institution+'-actual')
        # figure out logic for colors, set up second patch
        # set up legend
    fig.show()


if __name__ == "__main__":

    complete_metrics = {}

    metrics_files = os.listdir(METRICS_DIR)

    for mf in metrics_files:
        process_metrics(mf)

    graph(len(metrics_files))
