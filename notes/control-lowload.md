# Control run: low-load latency penalty (2026-09-30, kind on the workstation)

Question: is the higher p99 at low offered rates (seen in every configuration) JIT warm-up or a property of the host?

Procedure (config A, browse), run from `run/`:

```bash
python3 gen.py A | kubectl apply -f - && kubectl -n sut wait --for=condition=Available deploy --all --timeout=600s
../.venv/bin/python -c "
import campaign
campaign.push(campaign.targets('A','browse').encode(), '/work/browse.txt')
w = campaign.attack('/work/browse.txt', 60, workers=16); print('warm-up X=%.0f/s' % w['throughput'])
for r in (50, 200, 800, 50):
    rep = campaign.attack('/work/browse.txt', 20, rate=r)
    L = rep['latencies']; print(r, 'p50=%.2f p99=%.2f ms' % (L['50th']/1e6, L['99th']/1e6))
"
```

Output:

```
warm-up X=9598/s          (60 s closed loop, N=16: ~576,000 requests)
50 p50=1.28 p99=3.99 ms
200 p50=0.69 p99=2.15 ms
800 p50=0.44 p99=0.85 ms
50 p50=1.81 p99=4.75 ms   (back to 50/s after the 800/s step)
```

Conclusion: the penalty survives a warm-up far beyond JIT thresholds and returns when the rate drops again, so it is
not warm-up. Most likely CPU idle-state exit and thread wake-up latency in the Docker Desktop VM when requests arrive
20 ms apart. On bare metal: pin the frequency governor (`performance`) and limit C-states before the campaign.
