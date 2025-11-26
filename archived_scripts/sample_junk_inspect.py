from pathlib import Path
from fitparse import FitFile
from dotenv import load_dotenv
from os import getenv

load_dotenv(encoding='utf-8')
FIT_FOLDER = Path(getenv('FIT_FOLDER') or Path.cwd())
JUNK = FIT_FOLDER / "_junk"

if not JUNK.exists():
    print("_junk not found")
    raise SystemExit(0)

files = sorted(JUNK.glob('*.fit'))[:30]
print(f"Inspecting {len(files)} files from _junk (sample)")
for p in files:
    print('---')
    print(p.name, 'size=', p.stat().st_size)
    try:
        fit = FitFile(str(p))
        file_id_msgs = list(fit.get_messages('file_id'))
        if not file_id_msgs:
            print('file_id: <not found>')
        else:
            for m in file_id_msgs:
                # print all fields
                try:
                    fields = {}
                    for field in m.fields:
                        fields[field.name] = m.get_value(field.name)
                    print('file_id:', fields)
                except Exception as e:
                    print('file_id: error reading fields', e)
    except Exception as e:
        print('fitparse error:', e)
