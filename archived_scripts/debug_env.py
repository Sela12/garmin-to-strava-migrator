from pathlib import Path
from dotenv import dotenv_values, load_dotenv

# Determine repository root relative to this script and locate .env there
repo_root = Path(__file__).resolve().parent.parent
env_path = repo_root / '.env'
print(f'Loading from: {env_path}')
print(f'Exists: {env_path.exists()}')
if env_path.exists():
    print('Contents of .env:')
    try:
        print(env_path.read_text(encoding='utf-8'))
    except Exception as e:
        print(f'Error reading: {e}')

# Also load into environment for other scripts
load_dotenv(dotenv_path=env_path, encoding='utf-8')

values = dotenv_values(env_path)
print('Loaded values from dotenv_values():')
for k, v in values.items():
    if len(str(v)) > 30:
        print(f'  {k}={v[:30]}...')
    else:
        print(f'  {k}={v}')
