import sys
from http.server import BaseHTTPRequestHandler, HTTPServer
import json

class LogHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body   = self.rfile.read(length)
        print(f"\n{'='*60}")
        print(f"POST {self.path}  ({length} bytes)")
        print('='*60)
        try:
            data = json.loads(body)
            if isinstance(data, list):
                print(f"Batch of {len(data)} record(s):")
                for i, r in enumerate(data, 1):
                    print(f"  [{i}] level={r.get('level')} file={r.get('file')} line={r.get('line')} count={r.get('count')} msg={r.get('message','')[:60]}")
            else:
                print(json.dumps(data, indent=2))
        except Exception as e:
            print(body.decode(errors="replace"))
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")
    def log_message(self, fmt, *args):
        pass

port = int(sys.argv[1]) if len(sys.argv) > 1 else 8080
httpd = HTTPServer(("0.0.0.0", port), LogHandler)
print(f"[server] listening on :{port}")
httpd.serve_forever()
