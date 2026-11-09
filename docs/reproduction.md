# Reproducing the evidence

Install Python 3.11 and the pinned dependencies from `requirements-dev.txt` in a
virtual environment. No credentials, downloads of model weights, external APIs,
GPUs or paid resources are used. Installation requires access to PyPI.

```sh
python -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements-dev.txt
OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1 \
  python -m agent_trace run --output build/reproduction --runs 400 --concurrency 4
python -m agent_trace verify --output build/reproduction
```

On Windows set the three environment variables in PowerShell before running the
same Python commands. Use a fresh output directory. The maximum is 10,000 requests
and concurrency 32; this is a safety bound, not a recommended load benchmark.
Request latency includes instrumented work and synchronous export, but excludes
waiting for the request semaphore. Total elapsed time includes pipeline setup,
all execution and shutdown, and excludes verification. Trace IDs and timings vary.

In the shared project workspace, all load and benchmark executions must use the
coordinator's exclusive wrapper. The recorded run used this exact command from
the repository root:

```sh
OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1 \
python3 /Users/luchoduran/Sites/open-source-projects/.coordination/benchmark_lock.py \
  .venv/bin/python -m agent_trace run --output results/local --runs 400 --concurrency 4
```

The wrapper reported lock acquisition before execution. It is shared infrastructure,
not distributed with this standalone repository. Unit tests use tiny fixture runs,
not load tests. No benchmark is run by CI; CI verifies the recorded evidence.

## Recorded hardware

- Intel Xeon W-3223 CPU at 3.50 GHz, x86_64, 16 logical CPUs.
- Physical memory: 51,539,607,552 bytes (48 GiB).
- macOS 26.7.1; Python 3.11.13; OpenTelemetry SDK 1.39.1.
- CPU and memory read with `sysctl`; runtime versions recorded by the experiment.
- Four concurrent requests; model work uses `asyncio.to_thread`; native library
  thread environment limits were one. This pure Python classifier has no BLAS work.

This is one short run, without an uninstrumented control, repeated trials, energy
metering, saturation testing or isolated-machine guarantees. It establishes
executability and trace linkage, not comparative tracing overhead or throughput.
