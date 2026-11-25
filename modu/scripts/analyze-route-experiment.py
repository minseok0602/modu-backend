"""Aggregate the disclosed local measurements; optionally plot with Matplotlib."""
import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path
from statistics import mean


def analyze(directory: Path, plot: bool):
    with (directory / 'summary.csv').open(encoding='utf-8') as stream:
        rows = list(csv.DictReader(stream))
    groups = defaultdict(list)
    for raw in rows:
        row = {key: value if key in ('mode', 'scenario') else float(value) for key, value in raw.items()}
        groups[(row['scenario'], row['delay_ms'], row['concurrency'], row['mode'])].append(row)
    aggregate = []
    for (scenario, delay, concurrency, mode), runs in sorted(groups.items()):
        assert len(runs) == 3, (scenario, delay, concurrency, mode, len(runs))
        assert all(run['failures'] == 0 for run in runs), runs
        aggregate.append(dict(
            scenario=scenario, delay_ms=int(delay), concurrency=int(concurrency), mode=mode,
            repetitions=len(runs), successes=int(sum(run['successes'] for run in runs)),
            mean_run_p95_ms=mean(run['p95_ms'] for run in runs),
            min_run_p95_ms=min(run['p95_ms'] for run in runs),
            max_run_p95_ms=max(run['p95_ms'] for run in runs),
            mean_connection_ms_per_request=mean(run['connection_ms'] / run['successes'] for run in runs),
            mean_transaction_ms=mean(run['tx_mean_ms'] for run in runs),
            maximum_transaction_ms=max(run['tx_max_ms'] for run in runs),
            maximum_pending=int(max(run['pending_max'] for run in runs)),
            ai_calls=int(sum(run['ai_calls'] for run in runs)),
            kakao_calls=int(sum(run['kakao_calls'] for run in runs)),
            ai_calls_in_transaction=int(sum(run['ai_in_transaction'] for run in runs))))
    (directory / 'aggregate.json').write_text(json.dumps(aggregate, indent=2), encoding='utf-8')
    lines = ['| 시나리오 | AI 지연(ms) | 동시 요청 | 방식 | 반복별 p95 평균(ms) | 요청당 커넥션 점유 추정(ms) | 트랜잭션 평균(ms) |',
             '|---|---:|---:|---|---:|---:|---:|']
    for entry in aggregate:
        lines.append('| {scenario} | {delay_ms} | {concurrency} | {mode} | {mean_run_p95_ms:.1f} | '
                     '{mean_connection_ms_per_request:.1f} | {mean_transaction_ms:.1f} |'.format(**entry))
    (directory / 'aggregate.md').write_text('\n'.join(lines) + '\n', encoding='utf-8')
    print(json.dumps({'runs': len(rows), 'requests': sum(int(float(row['successes'])) for row in rows),
                      'failures': sum(int(float(row['failures'])) for row in rows)}, indent=2))
    if plot:
        draw(directory, aggregate)


def draw(directory, aggregate):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    plt.rcParams.update({'font.size': 10, 'axes.spines.top': False, 'axes.spines.right': False})
    fig, axes = plt.subplots(2, 2, figsize=(12, 8), layout='constrained')
    colors = {'before': '#c55342', 'after': '#157b81'}
    names = {'before': 'Before: one transaction', 'after': 'After: snapshot / apply'}
    for mode in ('before', 'after'):
        delayed = sorted((x for x in aggregate if x['scenario'] == 'ai_only' and x['delay_ms'] == 5000
                          and x['mode'] == mode), key=lambda x: x['concurrency'])
        zero = sorted((x for x in aggregate if x['scenario'] == 'ai_only' and x['delay_ms'] == 0
                       and x['mode'] == mode), key=lambda x: x['concurrency'])
        axes[0, 0].plot([x['concurrency'] for x in delayed],
                        [x['mean_connection_ms_per_request'] / 1000 for x in delayed], 'o-',
                        color=colors[mode], label=names[mode])
        y = [x['mean_run_p95_ms'] / 1000 for x in delayed]
        axes[0, 1].errorbar([x['concurrency'] for x in delayed], y,
            yerr=[[a - x['min_run_p95_ms'] / 1000 for a, x in zip(y, delayed)],
                  [x['max_run_p95_ms'] / 1000 - a for a, x in zip(y, delayed)]],
            fmt='o-', capsize=4, color=colors[mode], label=names[mode])
        axes[1, 1].plot([x['concurrency'] for x in zero], [x['mean_run_p95_ms'] for x in zero],
                        'o-', color=colors[mode], label=names[mode])
    with (directory / 'timeseries.csv').open(encoding='utf-8') as stream:
        samples = list(csv.DictReader(stream))
    for mode in ('before', 'after'):
        run = [x for x in samples if x['run'] == f'{mode}-ai_only-5000-10-1']
        axes[1, 0].plot([float(x['elapsed_ms']) / 1000 for x in run], [int(x['active']) for x in run],
                        color=colors[mode], linewidth=1.3, label=names[mode])
    axes[0, 0].set(title='AI delay 5 s: connection occupancy per request', xlabel='Concurrent requests',
                    ylabel='Connection-seconds / request (log scale)', yscale='log', xticks=[1, 5, 10])
    axes[0, 1].set(title='AI delay 5 s: service latency', xlabel='Concurrent requests',
                    ylabel='Mean of 3 run p95 values (s); bars = range', xticks=[1, 5, 10])
    axes[1, 0].set(title='AI delay 5 s, 10 requests: active connections', xlabel='Time since batch start (s)',
                    ylabel='Active connections', yticks=range(6))
    axes[1, 1].set(title='Zero provider delay: cost of transaction separation', xlabel='Concurrent requests',
                    ylabel='Mean of 3 run p95 values (ms)', xticks=[1, 5, 10])
    for ax in axes.flat:
        ax.grid(alpha=.2)
        ax.legend(fontsize=8)
    fig.suptitle('I:EUM local experiment | MySQL 8.0 | Hikari pool: 5 | real HTTP stub', fontsize=14)
    fig.savefig(directory / 'comparison.png', dpi=180)
    fig.savefig(directory / 'comparison.svg')
    plt.close(fig)
    (directory / 'plot-version.txt').write_text(f'Matplotlib {matplotlib.__version__}\n', encoding='utf-8')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('directory', type=Path)
    parser.add_argument('--plot', action='store_true')
    arguments = parser.parse_args()
    analyze(arguments.directory, arguments.plot)
