// A gzip front for the stand-in endpoint, for @smugglr/web's load run
// (NFR-SMUGGLR-010): it passes every request to <upstream> and answers
// with the upstream's response, its body compressed with gzip as HTTP
// Content-Encoding when the request accepts it. A dropped upstream
// connection drops the client's.
//
// Usage: node test/gzip.mjs <upstream>. Prints its own origin on stdout.

import { createServer, request } from 'node:http';
import { gzipSync } from 'node:zlib';

const upstream = new URL(process.argv[2]);

const server = createServer((incoming, outgoing) => {
  const forward = request(
    { host: upstream.hostname, port: upstream.port, method: incoming.method, path: incoming.url, headers: incoming.headers },
    (answer) => {
      const parts = [];
      answer.on('data', (part) => parts.push(part));
      answer.on('end', () => {
        let body = Buffer.concat(parts);
        const headers = { ...answer.headers };
        if (/\bgzip\b/.test(incoming.headers['accept-encoding'] ?? '')) {
          body = gzipSync(body);
          headers['content-encoding'] = 'gzip';
        }
        headers['content-length'] = String(body.length);
        outgoing.writeHead(answer.statusCode, headers);
        outgoing.end(body);
      });
      answer.on('error', () => outgoing.destroy());
    },
  );
  forward.on('error', () => outgoing.destroy());
  incoming.pipe(forward);
});

server.listen(0, '127.0.0.1', () => {
  process.stdout.write(`http://127.0.0.1:${server.address().port}\n`);
});
