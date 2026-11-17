#!/usr/bin/env python3
"""Φτιάχνει το index.html (σελίδα ανάγνωσης) από το roi.xml. Χρήση: python3 ftiaxe_selida.py [roi.xml] [index.html]"""
import html, sys, xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime

pigi = sys.argv[1] if len(sys.argv) > 1 else "roi.xml"
stoxos = sys.argv[2] if len(sys.argv) > 2 else "index.html"
ch = ET.parse(pigi).getroot().find("channel")
e = lambda s: html.escape(s or "")
gr = []
for it in ch.findall("item"):
    try:
        imer = parsedate_to_datetime(it.findtext("pubDate")).strftime("%d/%m/%Y")
    except Exception:
        imer = ""
    gr.append(f'<li><span class="imer">{imer}</span> <span class="kat">{e(it.findtext("category"))}</span>'
              f'<a href="{e(it.findtext("link"))}">{e(it.findtext("title"))}</a>'
              f'<p>{e(it.findtext("description"))}</p></li>')
try:
    enim = parsedate_to_datetime(ch.findtext("lastBuildDate")).strftime("%d/%m/%Y %H:%M")
except Exception:
    enim = ""
selida = f"""<!DOCTYPE html>
<html lang="el"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{e(ch.findtext("title"))}</title>
<link rel="alternate" type="application/rss+xml" title="{e(ch.findtext("title"))}" href="roi.xml">
<style>
body{{font-family:system-ui,-apple-system,"Segoe UI",sans-serif;max-width:760px;margin:2rem auto;padding:0 1rem;color:#1b1f24;line-height:1.5}}
h1{{font-size:1.35rem;margin-bottom:.2rem}} .ypo{{color:#57606a;font-size:.9rem;margin-top:0}}
ul{{list-style:none;padding:0}} li{{border-top:1px solid #d0d7de;padding:.8rem 0}}
.imer{{font-variant-numeric:tabular-nums;color:#57606a;font-size:.85rem}} .kat{{font-size:.8rem;background:#eef2f6;border-radius:4px;padding:1px 6px;margin:0 .4rem}}
a{{display:block;margin-top:.3rem;color:#0b5394;text-decoration:none;font-weight:600}} a:hover{{text-decoration:underline}}
p{{margin:.3rem 0 0;color:#424a53;font-size:.92rem}}
</style></head><body>
<h1>{e(ch.findtext("title"))}</h1>
<p class="ypo">{e(ch.findtext("description"))}<br>Τελευταία ενημέρωση: {enim} · Συνδρομή RSS: <code>roi.xml</code></p>
<ul>{''.join(gr) or '<li>Καμία εγγραφή.</li>'}</ul>
</body></html>
"""
open(stoxos, "w", encoding="utf-8").write(selida)
print(f"{stoxos}: {len(gr)} εγγραφές")
