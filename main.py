import asyncio
import httpx
import time
import gzip
import janus
import itertools
import io
import datetime
import time
import progressbar
import argparse
from warcio.warcwriter import WARCWriter
from warcio.statusandheaders import StatusAndHeaders

def grouper_it(n, iterable):
    it = iter(iterable)
    while True:
        chunk_it = itertools.islice(it, n)
        try:
            first_el = next(chunk_it)
        except StopIteration:
            return
        yield itertools.chain((first_el,), chunk_it)

def threaded_writer(sync_q, outpath):
    with open(outpath,'wb') as output:
        writer = WARCWriter(output,gzip=True)
        while True:
            res = sync_q.get()
            if res == True:
                sync_q.task_done()
                break

            record = writer.create_warc_record(
                res[0],
                'response',
                http_headers=StatusAndHeaders(f"{res[1]} {res[2]}", res[3], protocol='HTTP/1.1'),
                payload=io.BytesIO(res[4])
            )

            writer.write_record(record)

            sync_q.task_done()

async def get(client, url, queue):
    async with client.stream("GET", url) as res:
        b = bytearray()
        async for chunk in res.aiter_raw():
            b += chunk
        await queue.put((url, res.status_code, res.reason_phrase, dict(res.headers).items(), b))

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input", help="file with line-separated urls")
    parser.add_argument("output", help="warc file to write to")
    parser.add_argument("-s", "--chunk-size", help="how many requests to send out concurrently", type=int, default=50)
    parser.add_argument("--no-cert-verify", help="don't verify certificates", action="store_true")
    args = parser.parse_args()

    queue = janus.Queue()
    loop = asyncio.get_running_loop()
    fut = loop.run_in_executor(None, threaded_writer, queue.sync_q, args.output)

    with open(args.input) as f:
        lines = [l.rstrip() for l in f.readlines()]

    bar = progressbar.ProgressBar(max_value=len(lines))

    async with httpx.AsyncClient(http2=True,timeout=100000000000000,verify=not args.no_cert_verify) as client:
        i = 1
        for chunk in grouper_it(args.chunk_size,lines):
            chunk_start = time.monotonic()
            for c in asyncio.as_completed([get(client, l, queue.async_q) for l in chunk]):
                await c
                bar.update(i)
                i += 1

    await queue.async_q.put(True)

    await fut

    await queue.async_q.join()

    queue.close()
    await queue.wait_closed()

asyncio.run(main())
