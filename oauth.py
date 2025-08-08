import http.server
import socketserver
import threading
import webbrowser
from urllib.parse import urlparse, parse_qs, urlencode

class OAuthHandler(http.server.SimpleHTTPRequestHandler):
    """Handles OAuth2 redirect callback for local server."""
    def do_GET(self):
        parsed_path = urlparse(self.path)
        if parsed_path.path == '/callback':
            query = parse_qs(parsed_path.query)
            if "code" in query:
                self.server.code = query["code"][0]
                self.send_response(200)
                self.send_header('Content-type', 'text/html')
                self.end_headers()
                self.wfile.write(
                    b"<html><body><h1>Authorization successful!</h1>"
                    b"You can close this tab now.</body></html>"
                )
                # Graceful shutdown in a new thread
                threading.Thread(target=self.server.shutdown, daemon=True).start()
            else:
                self.send_error(400, "Missing code parameter")
        else:
            self.send_error(404)