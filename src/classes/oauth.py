import http.server
import socketserver
import threading
import logging
from urllib.parse import urlparse, parse_qs
from typing import Optional, cast


def _is_safe_return_url(url: Optional[str]) -> bool:
    if not url:
        return False
    u = str(url).strip()
    # Keep this intentionally strict to avoid open-redirect issues.
    return u.startswith("http://localhost") or u.startswith("http://127.0.0.1") or u.startswith("http://0.0.0.0")

class OAuthServer(socketserver.TCPServer):
    """Custom server to store the OAuth2 authorization code from redirection."""
    allow_reuse_address = True
    code: Optional[str] = None
    return_to_url: Optional[str] = None

    def __init__(self, server_address, handler_class, *, return_to_url: Optional[str] = None):
        super().__init__(server_address, handler_class)
        self.code = None
        self.return_to_url = return_to_url if _is_safe_return_url(return_to_url) else None


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
                return_to = server.return_to_url
                if return_to:
                    html = (
                        "<html><head>"
                        f"<meta http-equiv=\"refresh\" content=\"1;url={return_to}\">"
                        "</head><body>"
                        "<h1>Authorization successful!</h1>"
                        f"<p>Redirecting you back to the app… If it doesn't happen, <a href=\"{return_to}\">click here</a>.</p>"
                        "</body></html>"
                    )
                    self.wfile.write(html.encode("utf-8"))
                else:
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