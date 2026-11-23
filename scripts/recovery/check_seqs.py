import csv,glob,sys
d=sys.argv[1]
def seqs(f): return [int(r['seq']) for r in csv.DictReader(open(f))]
pub=seqs(f'{d}/publisher_0_latencies.csv')
ok = pub==list(range(pub[0],pub[-1]+1))
print('pub', len(pub), 'contiguous', ok)
for f in sorted(glob.glob(f'{d}/subscriber_*_latencies.csv')):
    s=seqs(f); dup=len(s)-len(set(s)); lo=max(pub[0],s[0]); hi=min(pub[-1],s[-1]); miss=sorted(x for x in set(pub)-set(s) if lo<=x<=hi)
    print(f.split('/')[-1], len(s), 'dups', dup, 'missing', len(miss), miss[:5], 'in order', s==sorted(s))
    ok = ok and dup==0 and not miss
print('RESULT', 'OK' if ok else 'NG')
