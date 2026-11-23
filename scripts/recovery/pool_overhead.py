import glob, os, sys
import pandas as pd
root = sys.argv[1]
rows = []
for v in ['orig', 'c', 'a', 'b']:
    for cfg in sorted(os.path.basename(d) for d in glob.glob(f'{root}/{v}/t*_s*')):
        d = f'{root}/{v}/{cfg}'
        pubs = glob.glob(f'{d}/iter_*/publisher_*_latencies.csv')
        subs = glob.glob(f'{d}/iter_*/subscriber_*_latencies.csv')
        if not pubs:
            continue
        p = pd.concat(pd.read_csv(f) for f in pubs)
        s = pd.concat(pd.read_csv(f) for f in subs)
        q = lambda x, k: x.quantile(k) / 1000
        rows.append(dict(variant=v, cfg=cfg, n_pub=len(p),
                         pub50=q(p.publish_latency_ns, .5), pub99=q(p.publish_latency_ns, .99), pub999=q(p.publish_latency_ns, .999),
                         e2e50=q(s.e2e_latency_ns, .5), e2e99=q(s.e2e_latency_ns, .99), e2e999=q(s.e2e_latency_ns, .999),
                         rcv50=q(s.receive_ipc_ns, .5), rcv999=q(s.receive_ipc_ns, .999)))
df = pd.DataFrame(rows)
pd.set_option('display.width', 200)
print(df.round(1).to_string(index=False))
df.to_csv(f'{root}/pooled.csv', index=False)
