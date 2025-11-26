from dotenv import load_dotenv
load_dotenv()
import os
import sys
from pathlib import Path
# Ensure project root is on sys.path so `strava_importer` package can be imported
sys.path.insert(0, str(Path.cwd()))
from strava_importer.auth import StravaAuth

c = os.getenv('CLIENT_ID')
s = os.getenv('CLIENT_SECRET')
code = os.getenv('AUTH_CODE')
print(f'CLIENT_ID present: {bool(c)}')
print(f'CLIENT_SECRET present: {bool(s)}')
print(f'AUTH_CODE present: {bool(code)}')

try:
    auth = StravaAuth(c, s, code)
    token = auth.exchange_code()
    print('EXCHANGE_OK')
    print(token)
except Exception as e:
    print('EXCEPTION:', type(e), e)
    resp = getattr(e, 'response', None)
    if resp is not None:
        try:
            print('STATUS:', resp.status_code)
            print('BODY:', resp.text)
        except Exception as ex:
            print('Failed to read response body:', ex)
    else:
        print('No response attached to exception')
