from pathlib import Path
from fitparse import FitFile
from dotenv import load_dotenv
from os import getenv

load_dotenv(encoding='utf-8')
FIT_FOLDER = Path(getenv('FIT_FOLDER') or Path.cwd())

counts = {}
missing = 0
total = 0
oldest_mtime_examples = []

for p in FIT_FOLDER.rglob('*.fit'):
    total += 1
    try:
        fit = FitFile(str(p))
        msgs = list(fit.get_messages('file_id'))
        if not msgs:
            missing += 1
            # collect mtime sample
            if len(oldest_mtime_examples) < 10:
                oldest_mtime_examples.append((p.name, p.stat().st_mtime))
            continue
        m = msgs[0]
        try:
            t = m.get_value('time_created')
        except Exception:
            t = None
        if t is None:
            missing += 1
            if len(oldest_mtime_examples) < 10:
                oldest_mtime_examples.append((p.name, p.stat().st_mtime))
            continue
        year = t.year
        counts[year] = counts.get(year, 0) + 1
    except Exception:
        missing += 1

print('Total FIT files:', total)
print('By year:')
for y in sorted(counts):
    print(y, counts[y])
print('Missing file_id.time_created:', missing)
print('\nSample of files missing file_id.time_created (name, mtime):')
for n,mt in oldest_mtime_examples:
    from datetime import datetime
    print(n, datetime.fromtimestamp(mt).isoformat())
