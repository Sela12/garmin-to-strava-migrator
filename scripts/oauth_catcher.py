import sys
import threading
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
from pathlib import Path
import time
import os

from dotenv import load_dotenv

load_dotenv(encoding='utf-8')

# Ensure project root importable
sys.path.insert(0, str(Path.cwd()))

from strava_importer.auth import StravaAuth


DEFAULT_PORT = 53682
DEFAULT_PATH = "/callback"


class OAuthHandler(BaseHTTPRequestHandler):
    server_version = "OAuthCatcher/1.0"

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path != DEFAULT_PATH:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"Not Found")
            return

        qs = parse_qs(parsed.query)
        code = qs.get("code", [None])[0]
        if not code:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b"Missing code param")
            return

        # Save code to server object and notify
        self.server.auth_code = code
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Authorization code received. You can close this window.")


def write_env_auth_code(env_path: Path, code: str):
    text = ""
    if env_path.exists():
        text = env_path.read_text(encoding="utf-8")

    lines = text.splitlines()
    found = False
    for i, line in enumerate(lines):
        if line.strip().startswith("AUTH_CODE="):
            lines[i] = f"AUTH_CODE={code}"
            found = True
            break
    if not found:
        lines.append(f"AUTH_CODE={code}")

    env_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_oauth_catcher():
    client_id = os.getenv("CLIENT_ID")
    client_secret = os.getenv("CLIENT_SECRET")
    redirect_uri = os.getenv("REDIRECT_URI", f"http://localhost:{DEFAULT_PORT}{DEFAULT_PATH}")

    if not client_id or not client_secret:
        print("CLIENT_ID and CLIENT_SECRET must be set in your .env")
        return 2

    parsed = urlparse(redirect_uri)
    port = parsed.port or DEFAULT_PORT
    path = parsed.path or DEFAULT_PATH

    if path != DEFAULT_PATH:
        print(f"Notice: using redirect path {path}. Server expects {DEFAULT_PATH}")

    server = HTTPServer(("", port), OAuthHandler)
    server.auth_code = None

    def serve():
        server.handle_request()  # handle a single request then exit

    t = threading.Thread(target=serve, daemon=True)
    t.start()

    auth_url = (
        f"https://www.strava.com/oauth/authorize?client_id={client_id}"
        f"&response_type=code&redirect_uri={redirect_uri}"
        f"&scope=activity:read,activity:write&approval_prompt=auto"
    )

    print("Opening browser to authorize the application. If nothing opens, visit this URL manually:")
    print(auth_url)
    webbrowser.open(auth_url)

    # Wait for code (timeout 300s)
    start = time.time()
    while time.time() - start < 300:
        if getattr(server, "auth_code", None):
            code = server.auth_code
            print("Received code:", code)
            # update .env
            env_path = Path.cwd() / ".env"
            write_env_auth_code(env_path, code)
            print(f"Wrote AUTH_CODE to {env_path}")

            # Exchange the code immediately to persist tokens
            try:
                auth = StravaAuth(client_id, client_secret, code, token_file=Path.cwd() / ".strava_tokens.json")
                token = auth.exchange_code()
                print("Token exchange successful. Access token saved to .strava_tokens.json")
                return 0
            except Exception as e:
                print("Token exchange failed:", e)
                resp = getattr(e, 'response', None)
                if resp is not None:
                    try:
                        print('STATUS:', resp.status_code)
                        print('BODY:', resp.text)
                    except Exception:
                        pass
                return 1
        time.sleep(0.5)

    print("Timeout waiting for authorization code")
    return 1


if __name__ == "__main__":
    rc = run_oauth_catcher()
    sys.exit(rc)
