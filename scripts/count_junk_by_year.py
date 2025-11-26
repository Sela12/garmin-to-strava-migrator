from pathlib import Path
from fitparse import FitFile
import datetime
from dotenv import load_dotenv
from os import getenv

load_dotenv(encoding='utf-8')
FIT_FOLDER = Path(getenv('FIT_FOLDER') or Path.cwd())
JUNK = FIT_FOLDER / "_junk"

if not JUNK.exists():
    print("_junk not found")
    raise SystemExit(0)

counts = {}
missing = 0

for p in JUNK.glob('*.fit'):
    try:
        fit = FitFile(str(p))
        msgs = list(fit.get_messages('file_id'))
        if not msgs:
            missing += 1
            continue
        m = msgs[0]
        try:
            t = m.get_value('time_created')
        except Exception:
            t = None
        if t is None:
            missing += 1
            continue
        year = t.year
        counts[year] = counts.get(year, 0) + 1
    except Exception:
        missing += 1

print('Totals by year (file_id.time_created):')
for y in sorted(counts):
    print(y, counts[y])
print('Missing file_id.time_created:', missing)
