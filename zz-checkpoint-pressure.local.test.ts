// Local measurement harness, not part of the suite: the chat store's writer and checkpoint worker
// under memory pressure. Works against main's owner and the branch's. Env: BENCH_MIN, BENCH_ROW_KB, BENCH_ROOT.
import { mkdirSync, rmSync, statSync, appendFileSync, readFileSync } from 'node:fs'
import { join } from 'node:path'
import { it } from 'vitest'
import Database from '../../../sqlite/sync-database'
import { JournalSqlBatch } from '../journal-sql-batch'
import { JournalCheckpointOwner } from './journal-checkpoint-owner'

const minutes = Number(process.env.BENCH_MIN ?? 10)
const rowBytes = Number(process.env.BENCH_ROW_KB ?? 2400) * 1024
const root = process.env.BENCH_ROOT ?? '/tmp/checkpoint-pressure-bench'
const out = join(root, 'result.jsonl')

function cgroup(): { current: number; high: number } {
  try {
    const path = readFileSync('/proc/self/cgroup', 'utf8').trim().split('::')[1]
    const dir = `/sys/fs/cgroup${path}`
    const current = Number(readFileSync(`${dir}/memory.current`, 'utf8'))
    const events = readFileSync(`${dir}/memory.events`, 'utf8')
    const high = Number(/^high (\d+)/m.exec(events)?.[1] ?? -1)
    return { current, high }
  } catch {
    return { current: -1, high: -1 }
  }
}

it('runs the writer and checkpoint worker under pressure', async () => {
  rmSync(root, { recursive: true, force: true })
  mkdirSync(root, { recursive: true })
  const filename = join(root, 'chat.db')
  const writer = new Database(filename)
  writer.exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA journal_size_limit=${128 * 1024 * 1024};
    CREATE TABLE writes(id INTEGER PRIMARY KEY, body BLOB);`)
  const warnings: string[] = []
  const warn = console.warn
  console.warn = (...args: unknown[]) => {
    warnings.push(args.map(String).join(' '))
    warn(...args)
  }
  const outages: unknown[] = []
  const owner = new JournalCheckpointOwner(writer, filename, {
    onOutage: (outage: unknown) => outages.push(outage)
  } as never)
  const batch = new JournalSqlBatch(writer, owner)
  const state = owner as unknown as {
    stopped: boolean
    blocked: boolean
    log: number
    truncate?: unknown
    current?: { busySince?: number }
    worker?: { current?: { busySince?: number } }
  }
  const attached = () => state.worker?.current ?? state.current
  const snapshot = () => ({
    stopped: state.stopped,
    blocked: state.blocked,
    log: state.log,
    truncating: !!state.truncate,
    late: attached()?.busySince !== undefined
  })
  const started = Date.now()
  const end = started + minutes * 60_000
  const batchMs: number[] = []
  const writeMs: number[] = []
  let id = 0
  let stoppedSamples = 0
  let lastSample = Date.now()
  let maxGap = 0
  let maxWal = 0
  let lastWal = 0
  let truncations = 0
  const startHigh = cgroup().high
  const walBytes = () => {
    try {
      return statSync(`${filename}-wal`).size
    } catch {
      return 0
    }
  }
  const sampler = setInterval(() => {
    const now = Date.now()
    maxGap = Math.max(maxGap, now - lastSample)
    if (state.stopped) stoppedSamples++
    lastSample = now
    const wal = walBytes()
    if (wal < lastWal / 2) truncations++
    lastWal = wal
    maxWal = Math.max(maxWal, wal)
    const memory = cgroup()
    appendFileSync(
      out,
      JSON.stringify({
        t: now - started,
        wal,
        rows: id,
        rss: process.memoryUsage().rss,
        cg: memory.current,
        high: memory.high,
        ...snapshot()
      }) + '\n'
    )
  }, 1000)
  // Old rows are deleted as new ones land, so the store stays bounded while the WAL churns.
  const body = Buffer.alloc(rowBytes, 7)
  while (Date.now() < end) {
    const t0 = performance.now()
    let w0 = 0
    await batch.run([++id], (row) => {
      w0 = performance.now()
      writer.prepare('INSERT INTO writes VALUES(?, ?)').run(row, body)
      writer.prepare('DELETE FROM writes WHERE id < ?').run(row - 400)
    })
    const ms = performance.now() - t0
    batchMs.push(ms)
    writeMs.push(w0 ? performance.now() - w0 : 0)
    if (ms > 2000) {
      console.log('BENCH-STALL ' + JSON.stringify({ t: Date.now() - started, ms: Math.round(ms), ...snapshot() }))
    }
    await new Promise((resolve) => setImmediate(resolve))
  }
  clearInterval(sampler)
  const tailStart = Date.now()
  // After the run: does the WAL come back under its limit within 30 s of quiet?
  while (Date.now() - tailStart < 30_000 && walBytes() > 128 * 1024 * 1024) {
    owner.afterCommit()
    await new Promise((resolve) => setTimeout(resolve, 500))
  }
  const pct = (values: number[], p: number) => {
    const sorted = values.toSorted((a, b) => a - b)
    return Math.round(sorted[Math.min(sorted.length - 1, Math.floor(p * sorted.length))] ?? 0)
  }
  const summary = {
    summary: true,
    minutes,
    rows: id,
    batchP50: pct(batchMs, 0.5),
    batchP99: pct(batchMs, 0.99),
    batchMax: Math.round(Math.max(...batchMs)),
    writeP99: pct(writeMs, 0.99),
    maxLoopGapMs: maxGap,
    stoppedSamples,
    stoppedAtEnd: state.stopped,
    workerAtEnd: !!(attached() ?? state.worker),
    truncations,
    maxWal,
    finalWal: walBytes(),
    cgroupHighEvents: cgroup().high - startHigh,
    warnings,
    outages
  }
  appendFileSync(out, JSON.stringify(summary) + '\n')
  console.log('BENCH-SUMMARY ' + JSON.stringify(summary))
  await owner.close()
  writer.close()
  console.warn = warn
}, 3 * 60 * 60_000)
