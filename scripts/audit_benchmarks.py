#!/usr/bin/env python3
"""Validate saved raw arrival/accounting data; this does not prove register linearizability."""
import argparse
import collections
import gzip
import json
from pathlib import Path

def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('inputs',nargs='+',type=Path);args=parser.parse_args()
    total=0;runs=0;categories=collections.Counter()
    for directory in args.inputs:
        for path in sorted(directory.glob('n*.summary.json')):
            summary=json.loads(path.read_text());spec=summary['spec'];counts=collections.Counter()
            samples=path.with_name(path.name.replace('.summary.json','.samples.jsonl.gz'))
            with gzip.open(samples,'rt') as source:
                for seq,line in enumerate(source):
                    item=json.loads(line);assert item['seq']==seq,(path,seq)
                    assert item['phase']=='measure'
                    assert item['scheduled_ns']==seq*1_000_000_000//spec['rate']
                    assert item['completed_ns']>=item['scheduled_ns']
                    if item['dispatched_ns'] is None:
                        assert item['error']=='load generator queue full';counts['rejected']+=1
                    else:
                        assert item['completed_ns']>=item['dispatched_ns']>=item['scheduled_ns'];counts['dispatched']+=1
                        if item['status']==0:counts['uncertain_delivery']+=1
                    counts['offered']+=1
                    if item['error'] is None:
                        counts['successes']+=1
                        if item['completed_ns']<=summary['window_seconds']*1e9:counts['successful_completions_in_window']+=1
                    else:
                        counts['errors']+=1
                        if item['dispatched_ns'] is not None:categories[(item['status'],item['error'])]+=1
            for key in ('offered','successes','errors','rejected','successful_completions_in_window'):assert summary[key]==counts[key],(path,key,summary[key],counts[key])
            assert summary['settled'],path
            counters=summary['server_metrics_delta']
            if not counts['uncertain_delivery']:
                assert sum(m['http_requests'] for m in counters)==counts['dispatched'],path
            if summary['errors']==summary['rejected']:
                assert sum(m['rpc_requests_sent']-m['rpc_requests_received'] for m in counters)==0,(path,'request frame boundary')
                assert sum(m['rpc_replies_sent']-m['rpc_replies_received'] for m in counters)==0,(path,'reply frame boundary')
            assert all(s['metrics']['work_pending']==0 for s in summary['resources_before']+summary['resources_after']),path
            total+=counts['offered'];runs+=1
    print(json.dumps({'runs':runs,'raw_samples':total,'other_error_categories':[{ 'status':s,'error':e,'count':n} for (s,e),n in categories.items()]},indent=2))
if __name__=='__main__':main()
