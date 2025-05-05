#!/usr/bin/env python3
r'''
Interactive converter: txt to PostgreSQL‑ready CSV with auto-separator detection.
‑ Запускается без аргументов.
‑ Определяет возможный разделитель по первой строке (|, ;, tab, и т.п.).
‑ Спрашивает, подтверждаете ли или вводите вручную (с подсказкой про экранирование).
‑ Пустые значения → \N (NULL для PostgreSQL).
‑ Поддерживает файлы и URL, лог + прогресс.
'''

from __future__ import annotations

import csv, datetime as dt, itertools, logging, pathlib, re, shutil, sys, tempfile, urllib.request
from types import SimpleNamespace
from typing import List

try:
    from tqdm import tqdm
except ModuleNotFoundError:
    def tqdm(iterable=None, total=None, unit=None, unit_scale=False, desc=None):
        if iterable is None:
            return range(total or 0)
        return iterable

NULL = r'\N'
ENCODING_IN  = 'utf-8'
ENCODING_OUT = 'utf-8-sig'
COMMON_SEPARATORS = ['|', ',', ';', '\t']

def _safe_iso(raw: str) -> str:
    raw = raw.strip()
    try:
        d, m, y = raw.split('.')
        if len(y) == 4:
            return dt.date(int(y), int(m), int(d)).isoformat()
    except Exception:
        pass
    return NULL

def _ask(prompt: str, default: str | None = None) -> str:
    suffix = f' [{default}]' if default is not None else ''
    while True:
        val = input(f'{prompt}{suffix}: ').strip()
        if val:
            return val
        if default is not None:
            return default
        print('⛔ Пустой ввод недопустим.')

def _select_file() -> str:
    while True:
        p = input('Введите путь к .txt файлу, URL или директорию: ').strip()
        if not p:
            print('⛔ Путь не может быть пустым.'); continue
        path = pathlib.Path(p)
        if path.is_file():
            return str(path.resolve())
        if path.is_dir():
            txt_files = sorted(f for f in path.iterdir() if f.suffix.lower() == '.txt')
            if not txt_files:
                print('⛔ В каталоге нет .txt файлов.'); continue
            print('Найденные .txt файлы:')
            for i, f in enumerate(txt_files, 1):
                print(f'{i}) {f.name}')
            idx = int(_ask('Выберите номер файла', default='1'))
            return str(txt_files[idx-1].resolve())
        if p.startswith(('http://', 'https://')):
            return p
        print('⛔ Не удалось интерпретировать ввод.')

def _get_source(path_or_url: str) -> SimpleNamespace:
    p = pathlib.Path(path_or_url)
    if p.is_file():
        return SimpleNamespace(path=p.resolve(), cleanup=lambda: None)
    if path_or_url.startswith(('http://', 'https://')):
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.txt')
        logging.info('Скачиваю %s …', path_or_url)
        with urllib.request.urlopen(path_or_url) as resp, open(tmp.name, 'wb') as out:
            shutil.copyfileobj(resp, out)
        return SimpleNamespace(path=pathlib.Path(tmp.name), cleanup=lambda: pathlib.Path(tmp.name).unlink())
    sys.exit('⛔ Не найден файл и не URL: ' + path_or_url)

def _guess_separator(first_line: str) -> str:
    counts = {sep: first_line.count(sep if sep != '\\t' else '\t') for sep in COMMON_SEPARATORS}
    best = max(counts.items(), key=lambda x: x[1])
    return best[0]

def _process(src: pathlib.Path, dst: pathlib.Path, sep_regex: re.Pattern[str], columns: List[str]) -> None:
    logging.info('Читаю из: %s', src)
    logging.info('Пишу  в : %s', dst)
    total = src.stat().st_size
    date_idx = {i for i, c in enumerate(columns) if c.lower() in {'birthday', 'date'}}
    with src.open('r', encoding=ENCODING_IN, errors='ignore') as fin, dst.open('w', newline='', encoding=ENCODING_OUT) as fout:
        w = csv.writer(fout)
        w.writerow(columns)
        with tqdm(total=total, unit='B', unit_scale=True, desc='Обработка') as bar:
            for line in fin:
                bar.update(len(line.encode(ENCODING_IN)))
                fields = sep_regex.split(line.rstrip('\n'))
                fields = list(itertools.islice(fields + ['']*len(columns), len(columns)))
                fields = [f.strip() if f.strip() else NULL for f in fields]
                for idx in date_idx:
                    fields[idx] = _safe_iso(fields[idx])
                w.writerow(fields)
    logging.info('✓ Готово. Записано %s', dst.name)

def main():
    logging.basicConfig(format='[%(levelname)s] %(message)s', level=logging.INFO)
    if len(sys.argv) > 2:
        sys.exit('Использование: python txt2pgcsv.py [файл_или_URL]')

    path_or_url = sys.argv[1] if len(sys.argv) == 2 else _select_file()
    source = _get_source(path_or_url)

    # автоопределение разделителя
    with source.path.open('r', encoding=ENCODING_IN, errors='ignore') as f:
        first_line = f.readline()
    suggested = _guess_separator(first_line)
    human_suggest = {'|': r'\|', ',': ',', ';': ';', '\t': r'\t'}[suggested]
    print(f'🔍 Предполагаемый разделитель: {suggested!r}')
    print(f'Если согласны — просто нажмите Enter. Если другой — введите вручную.')
    print('⚠️  Если вы используете | или другой спецсимвол — экранируйте его (например: \\| или \\t)')
    sep_raw = _ask('Введите разделитель (regex)', default=human_suggest)
    sep_regex = re.compile(rf'\s*{sep_raw}\s*')

    cols_raw = _ask('Введите названия колонок через запятую (в порядке следования)')
    columns = [c.strip() for c in cols_raw.split(',') if c.strip()]
    if not columns:
        sys.exit('⛔ Не получено ни одной колонки — завершаю.')

    dst = source.path.with_name(f'{source.path.stem}_pg.csv')
    try:
        _process(source.path, dst, sep_regex, columns)
    finally:
        source.cleanup()

if __name__ == '__main__':
    main()