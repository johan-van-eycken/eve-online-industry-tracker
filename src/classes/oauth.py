import http.server
import socketserver
import threading
import logging
from urllib.parse import urlparse, parse_qs
from typing import Optional, cast

class OAuthServer(socketserver.TCPServer):
    """Custom server to store the OAuth2 authorization code from redirection."""
    allow_reuse_address = True
    code: Optional[str] = None

    def __init__(self, server_address, handler_class):
        super().__init__(server_address, handler_class)
        self.code = None


class OAuthHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        parsed_url = urlparse(self.path)
        if parsed_url.path == "/favicon.ico":  # Ignore favicon requests
            self.send_response(200)
            self.send_header("Content-type", "image/x-icon")
            self.end_headers()
            return
        if parsed_url.path == "/callback":  # Ensure callback path matches redirect_uri
            query = parse_qs(parsed_url.query)
            if "code" in query:
                server = cast(OAuthServer, self.server)
                server.code = query["code"][0]  # Extract the authorization code
                self.send_response(200)
                self.send_header("Content-type", "text/html")
                self.end_headers()
                self.wfile.write(
                    b"<html><body><h1>Authorization successful!</h1>"
                    b"You can close this tab now.</body></html>"
                )
                logging.info("Authorization code received and server shutting down.")
                threading.Thread(target=self.server.shutdown, daemon=True).start()  # Graceful shutdown
            else:
                self.send_error(400, "Authorization code missing in callback.")
        else:
            self.send_error(404, "Unknown path.")