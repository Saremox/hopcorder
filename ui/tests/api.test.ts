import { describe, expect, it } from 'vitest'
import { APIError, Client, apiBase, queryURL } from '../src/api'

describe('apiBase', () => {
  it('is relative to the UI, under any proxy prefix', () => {
    expect(apiBase('http://h:8080/ui/').href).toBe('http://h:8080/api/v1/query/')
    expect(apiBase('http://h:8080/ui/#/stream?agent=a').href).toBe('http://h:8080/api/v1/query/')
    expect(apiBase('https://h/net/hopcorder/ui/index.html?x=1').href).toBe('https://h/net/hopcorder/api/v1/query/')
    expect(apiBase('http://localhost:5173/').href).toBe('http://localhost:5173/api/v1/query/')
  })

  it('encodes parameters and drops empty ones', () => {
    expect(queryURL(new URL('http://h/api/v1/query/'), 'cycles', { agent: 'a&b', target: 't', protocol: '', limit: 1, x: undefined })).toBe(
      'http://h/api/v1/query/cycles?agent=a%26b&target=t&limit=1',
    )
  })
})

type Call = { url: URL }

function fakeClient(respond: (u: URL) => { status?: number; body: unknown }) {
  const calls: Call[] = []
  const fetcher = (async (input: RequestInfo | URL) => {
    const url = new URL(String(input))
    calls.push({ url })
    const r = respond(url)
    return new Response(JSON.stringify(r.body), { status: r.status ?? 200 })
  }) as typeof fetch
  return { client: new Client(new URL('http://h/api/v1/query/'), fetcher), calls }
}

const stream = { agent: 'lab', target: 'dc1', protocol: 'icmp' }
const range = { from: 1000, to: 100_000 }

describe('Client', () => {
  it('finds the cycle at a moment, else the first after it', async () => {
    const { client, calls } = fakeClient((u) =>
      u.searchParams.get('order') === 'asc' ? { body: { rows: [{ seq: 2 }] } } : { body: { rows: [] } },
    )
    const c = await client.cycleAt(stream, range, 50_000)
    expect(c?.seq).toBe(2)
    expect(calls.map((c) => Object.fromEntries(c.url.searchParams))).toEqual([
      { agent: 'lab', target: 'dc1', protocol: 'icmp', from: '1000', to: '50001', limit: '1' },
      { agent: 'lab', target: 'dc1', protocol: 'icmp', from: '50000', to: '100000', order: 'asc', limit: '1' },
    ])
  })

  it('does not ask past the range', async () => {
    const { client, calls } = fakeClient(() => ({ body: { rows: [] } }))
    expect(await client.cycleAfter(stream, range, 100_000)).toBeNull()
    expect(await client.cycleBefore(stream, range, 1000)).toBeNull()
    expect(calls).toHaveLength(0)
  })

  it('reports the server error', async () => {
    const { client } = fakeClient(() => ({ status: 400, body: { error: 'agent and target are required' } }))
    await expect(client.topology(stream, range)).rejects.toThrow(new APIError('topology: agent and target are required', 400))
  })

  it('reports a non-JSON failure', async () => {
    const fetcher = (async () => new Response('bad gateway', { status: 502 })) as unknown as typeof fetch
    await expect(new Client(new URL('http://h/api/v1/query/'), fetcher).targets(range)).rejects.toThrow('targets: HTTP 502')
  })
})
