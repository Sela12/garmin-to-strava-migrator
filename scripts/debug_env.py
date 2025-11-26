from pathlib import Path
from dotenv import dotenv_values
import os

os.chdir(r'c:\Users\סלע נץ\Documents\strava')
env_path = Path.cwd() / '.env'
print(f'Loading from: {env_path}')
print(f'Exists: {env_path.exists()}')
if env_path.exists():
    print(f'Contents of .env:')
    try:
        print(env_path.read_text(encoding='utf-8'))
    except Exception as e:
        print(f'Error reading: {e}')

values = dotenv_values(env_path)
print('Loaded values from dotenv_values():')
for k, v in values.items():
    if len(str(v)) > 30:
        print(f'  {k}={v[:30]}...')
    else:
        print(f'  {k}={v}')
