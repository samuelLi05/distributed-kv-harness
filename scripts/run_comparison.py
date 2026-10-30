#!/usr/bin/env python3
"""Run the declared comparison serially on one host; preserve a resumable study manifest."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys

PROTOCOLS='abd,gus,hermes_static,hermes_alr_static'
def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',required=True,type=Path)
    parser.add_argument('--protocols',default=PROTOCOLS)
    parser.add_argument('--seconds',type=float,default=15)
    parser.add_argument('--warmup',type=float,default=3)
    parser.add_argument('--repeats',type=int,default=5)
    parser.add_argument('--keys',type=int,default=100000)
    parser.add_argument('--latency-keys',type=int,default=10000)
    parser.add_argument('--resume',action='store_true')
    args=parser.parse_args();root=args.output.resolve()
    root.mkdir(parents=True,exist_ok=args.resume)
    protocols=args.protocols.split(',')
    common=['--protocols',args.protocols,'--seconds',str(args.seconds),'--warmup',str(args.warmup),'--repeats',str(args.repeats)]
    studies=[('core',['--nodes','3,6,9','--rates','250,4000','--hot-low-rate','100','--workloads','read_heavy,mixed,write_heavy,hot_mixed','--keys',str(args.keys)],3*len(protocols)*4*2*args.repeats)]
    for ingress,rtt in [('owner',0),('owner',2),('random',2),('owner',20),('random',20)]:
        studies.append((f'latency-{ingress}-{rtt}',['--nodes','6','--rates','250','--workloads','read_heavy,write_heavy','--ingress',ingress,'--peer-rtt-ms',str(rtt),'--keys',str(args.latency_keys)],len(protocols)*2*args.repeats))
    studies.append(('delete-recreate',['--nodes','3','--rates','250','--workloads','mixed','--keys','1000','--delete-percent','50'],len(protocols)*args.repeats))
    for ingress in ['owner','random']:
        studies.append((f'latency-stress-{ingress}-20',['--nodes','6','--rates','4000','--workloads','read_heavy','--ingress',ingress,'--peer-rtt-ms','20','--workers','256','--keys',str(args.latency_keys)],len(protocols)*args.repeats))
    manifest_path=root/'study.json'
    manifest={'binaries_sha256':{name:hashlib.sha256(Path('target/release',name).read_bytes()).hexdigest() for name in ('distributed-kv-harness','kv-bench')},'arguments':{k:str(v) if isinstance(v,Path) else v for k,v in vars(args).items()},'studies':[]}
    for name,options,expected in studies:
        command=[sys.executable,'scripts/benchmark.py','--output',str(root/name),*common,*options]
        manifest['studies'].append({'name':name,'command':command,'expected_runs':expected})
    if args.resume:
        previous=json.loads(manifest_path.read_text())
        assert previous['binaries_sha256']==manifest['binaries_sha256'], 'binary build changed; start a new study'
        assert [(s['name'],s['command'],s['expected_runs']) for s in previous['studies']]==[(s['name'],s['command'],s['expected_runs']) for s in manifest['studies']], 'resume parameters differ'
    manifest_path.write_text(json.dumps(manifest,indent=2))
    for study in manifest['studies']:
        target=root/study['name']
        command=study['command']
        if target.exists():
            results=target/'results.json'
            inactive=all(not json.loads(p.read_text())["active"] for p in target.glob('processes-*.json'))
            if args.resume and inactive and results.exists() and len(json.loads(results.read_text()))==study['expected_runs']:
                print(f"Skipping complete study {study['name']}",flush=True);continue
            if not args.resume or not inactive:
                raise RuntimeError(f"Study at {target} exists or has an active process manifest; reconcile it before resuming")
            command=[*command,'--resume']
        print(f"Starting {study['name']} ({study['expected_runs']} measured runs)",flush=True)
        subprocess.run(command,check=True)
    paths=[str(root/s['name']) for s in manifest['studies']]
    subprocess.run([sys.executable,'scripts/compare_protocols.py',*paths,'--output',str(root/'report')],check=True)
    with (root/'audit.json').open('w') as output:
        subprocess.run([sys.executable,'scripts/audit_benchmarks.py',*paths],stdout=output,check=True)
    print(f"Completed study: {root}",flush=True)
if __name__=='__main__':main()
