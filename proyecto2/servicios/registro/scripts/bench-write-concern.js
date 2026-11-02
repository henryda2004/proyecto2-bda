const database = db.getSiblingDB("bench_tmp");
const coll = database.getCollection("writes");
const N = 1000;
const WARMUP = 100;
const now = () => (typeof performance !== "undefined" ? performance.now() : Date.now());

function percentile(sorted, p) {
  const idx = Math.ceil((p / 100) * sorted.length) - 1;
  return sorted[Math.max(0, idx)];
}

function run(label, writeConcern) {
  for (let i = 0; i < WARMUP; i++) {
    coll.insertOne({ warmup: true }, { writeConcern: writeConcern });
  }
  const times = [];
  for (let i = 0; i < N; i++) {
    const t0 = now();
    coll.insertOne({ n: i, label: label }, { writeConcern: writeConcern });
    times.push(now() - t0);
  }
  times.sort((a, b) => a - b);
  const sum = times.reduce((a, b) => a + b, 0);
  return {
    write_concern: label,
    samples: N,
    mean_ms: Number((sum / N).toFixed(3)),
    p50_ms: Number(percentile(times, 50).toFixed(3)),
    p95_ms: Number(percentile(times, 95).toFixed(3)),
    p99_ms: Number(percentile(times, 99).toFixed(3))
  };
}

const result = {
  executed_at: new Date().toISOString(),
  results: [
    run("w1", { w: 1 }),
    run("majority", { w: "majority" })
  ]
};

database.dropDatabase();
print(JSON.stringify(result, null, 2));