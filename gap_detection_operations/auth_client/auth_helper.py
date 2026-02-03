import warnings
warnings.filterwarnings("ignore")
import argparse
import configparser
import http.server
import json
import secrets
import urllib.parse
import urllib.request
import webbrowser
from pathlib import Path

class CallbackHandler(http.server.BaseHTTPRequestHandler):
    code = None
    def do_GET(self):
        params = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        
        if "code" in params:
            CallbackHandler.code = params["code"][0]
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"Authorization successful. Return to terminal.")
        else:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b"Authorization failed.")
    
    def log_message(self, *args):
        pass

def load_config(path: str) -> dict:
    config = configparser.ConfigParser()
    config.read(Path(path).expanduser())
    
    if "auth" not in config:
        raise ValueError("Missing [auth] section in config file")
    
    auth = config["auth"]
    for field in ["client_id", "idp_host", "token_file"]:
        if field not in auth:
            raise ValueError(f"Missing required config field: {field}")
    
    return dict(auth)

def main():
    parser = argparse.ArgumentParser(description="Gap Detection API authentication helper")
    parser.add_argument("--config", default="./config.ini", help="Path to config.ini")
    args = parser.parse_args()
    
    config = load_config(args.config)
    
    adfs_base = f"https://{config['idp_host']}/adfs"
    authorize_url = f"{adfs_base}/oauth2/authorize/"
    token_url = f"{adfs_base}/oauth2/token/"
    
    state = secrets.token_urlsafe(32)
    
    # Build authorization URL
    auth_params = {
        "client_id": config["client_id"],
        "response_type": "code",
        "redirect_uri": "http://127.0.0.1:8080/callback",
        "state": state,
    }
    auth_url = f"{authorize_url}?{urllib.parse.urlencode(auth_params)}"
    
    # Start callback server
    server = http.server.HTTPServer(("127.0.0.1", 8080), CallbackHandler)
    
    print("Opening browser for authentication...")
    webbrowser.open(auth_url)
    server.handle_request()
    server.server_close()
    
    if not CallbackHandler.code:
        print("Authorization failed - no code received")
        return 1
    
    # Exchange authorization code for access token
    token_data = urllib.parse.urlencode({
        "grant_type": "authorization_code",
        "client_id": config["client_id"],
        "code": CallbackHandler.code,
        "redirect_uri": "http://127.0.0.1:8080/callback",
    }).encode()
    
    req = urllib.request.Request(token_url, data=token_data)
    with urllib.request.urlopen(req) as response:
        tokens = json.loads(response.read().decode())
    
    # Save access token
    token_path = Path(config["token_file"]).expanduser()
    token_path.parent.mkdir(parents=True, exist_ok=True)
    token_path.write_text(tokens["access_token"])
    
    print(f"Access token saved to {token_path}")
    return 0

if __name__ == "__main__":
    exit(main())
