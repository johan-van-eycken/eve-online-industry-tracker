import http.server
import socketserver
import threading
import logging
from urllib.parse import urlparse, parse_qs
from typing import cast, Optional

class OAuthServer(socketserver.TCPServer):
    """Custom server to store OAuth code from OAuth2 redirect."""
    allow_reuse_address = True  # Helps avoid address-in-use errors
    code: Optional[str]  # Annotate code as Optional[str]

    def __init__(self, server_address, RequestHandlerClass):
        super().__init__(server_address, RequestHandlerClass)
        self.code = None

class OAuthHandler(http.server.SimpleHTTPRequestHandler):
    """Handles OAuth2 redirect callback for local server."""

    def do_GET(self):
        parsed_path = urlparse(self.path)
        if parsed_path.path == '/callback':
            query = parse_qs(parsed_path.query)
            if "code" in query:
                # Cast server to OAuthServer for type checking
                server = cast(OAuthServer, self.server)
                server.code = query["code"][0]
                self.send_response(200)
                self.send_header('Content-type', 'text/html')
                self.end_headers()
                self.wfile.write(
                    b"<html><body><h1>Authorization successful!</h1>"
                    b"You can close this tab now.</body></html>"
                )
                logging.info("OAuth code received, shutting down server.")
                # Graceful shutdown in a new thread
                threading.Thread(target=self.server.shutdown, daemon=True).start()
            else:
                logging.warning("Callback received without code parameter.")
                self.send_error(400, "Missing code parameter")
        else:
            logging.warning(f"Unknown path requested: {parsed_path.path}")
            self.send_error(404)