#!/usr/bin/env python3
"""Παρακολούθηση επίσημων πηγών για το Τεχνικό Γραφείο Μουδιώτης (16/11/2026).

Πηγές (όλες από κέλυφος νέφους με curl, επαληθευμένο 16/11/2026):
  A. ΥΜΕ, Δελτία Τύπου: ροή RSS (Joomla, limit=50) · νέες εγγραφές + φίλτρο θεματικής
  B. ΥΜΕ, ενότητες οχημάτων (Τεχνολογία Οχημάτων κ.ά.): ανίχνευση σελίδων· σύγκριση κειμένου και
     συνημμένων με την προηγούμενη κατάσταση (η ροή RSS της ενότητας ΔΕΝ πιάνει αλλαγές συνημμένων)
  C. Διαύγεια, ΥΠΟΜΕ (100025905): όλες οι πράξεις από τον τελευταίο έλεγχο · φίλτρο θέματος
  D. Εθνικό Μητρώο Διοικητικών Διαδικασιών: οι διαδικασίες της λίστας του γραφείου (lista.tsv)·
     εντοπισμός σελίδας με αναζήτηση κωδικού, σύγκριση ουσίας (η γραμμή «Τελευταία ενημέρωση» εξαιρείται)

Χρήση:
  python3 parakolouthisi_pigon.py --katastasi katastasi.json --lista lista.tsv --exodos <φάκελος>
          [--apo ΕΕΕΕ-ΜΜ-ΗΗ] [--mono A,B,C,D]
Έξοδος στον φάκελο: eurimata.json, anafora.md, roi.xml (δική μας ροή RSS, τελευταίες 30 εγγραφές),
piges.opml · η κατάσταση ενημερώνεται στη θέση της (συμπαγής, μία τιμή ανά γραμμή, < 25 χιλ. tokens).
Ψευδείς αλλαγές που αποκλείστηκαν (16/11/2026): itemAuthorLatest/itemNavigation του K2, cloak email,
μετρητής επισκέψεων, παροδική απόκριση (διπλή επιβεβαίωση), «Τελευταία ενημέρωση» του Μητρώου.

Πρώτη εκτέλεση (χωρίς κατάσταση) για B και D = γραμμή βάσης: καταγράφει, δεν αναφέρει «αλλαγές».
Κωδικός εξόδου: 0 εντάξει · 3 κάποια πηγή δεν διαβάστηκε πλήρως (το λέει η αναφορά) · 2 σφάλμα χρήσης.
"""
import argparse, subprocess, concurrent.futures as cf, datetime as dt, hashlib, html, json, re, sys, time, unicodedata
import urllib.parse, urllib.request
import xml.etree.ElementTree as ET
from email.utils import format_datetime, parsedate_to_datetime
from pathlib import Path

UA = {"User-Agent": "curl/8.5.0"}
YME = "https://www.yme.gov.gr"
YME_DT_RSS = YME + "/2013-01-31-06-37-23/2013-01-31-07-00-49?format=feed&type=rss&limit=50"
YME_ENOTITES = ["/metafores/texnologia-oximaton",
                "/odigos-politi/aytokinita",
                "/odigos-politi/texnikos-elegxos-oximaton",
                "/metafores/texnikos-elegxos-egkatastaseis-eksypiretisis-oximaton"]
YME_MAX_SELIDES = 600
DIAVGEIA = "https://diavgeia.gov.gr/opendata/search.json"
YPOME = "100025905"
MITOS = "https://mitos.gov.gr/index.php"
ATHINA = dt.timezone(dt.timedelta(hours=2))
ROI_URL = "https://dimoud.github.io/roi-texniko-grafeio/roi.xml"  # δημόσια διεύθυνση της δικής μας ροής (GitHub Pages)

# Θεματικό φίλτρο (κείμενο χωρίς τόνους, πεζά)
THEMA = re.compile(
    r"ecoc|πιστοποιητικ\w* συμμορφωσ|εγκρισ\w* τυπου|κοινοποιησ\w* εγκρισ|ταξινομ|ρυμουλκ|τροχοσπιτ|αυτοκινουμ|καντιν|"
    r"ειδικης χρησης|ειδικου σκοπου|\bεχεσ\b|\bμοικ\b|κτεο|τεχνικ\w* ελεγχ\w* οχημ|αδει\w* κυκλοφορ|πινακιδ\w* κυκλοφορ|"
    r"κρατικ\w* πινακιδ|τελους σειρ|μικρης σειρ|μεμονωμεν\w* εγκρισ|\bταο\b|τεχνικο αρχειο οχημ|858/2018|168/2013|"
    r"euro ?7|μετοικ|μηχανηματ\w* εργου|29900|191277|2805/2021|54795|μεταχειρισμ\w* οχημ|εισαγ\w* οχημ|"
    r"οχηματ\w* ειδικ|ταχογραφ|\badr\b|ηλεκτροκινησ|ηλεκτρικ\w* οχημ")
TYPOI = {"Α.1": "νόμος/ΠΔ", "Α.2": "κανονιστική πράξη", "Α.3": "εγκύκλιος", "Α.4": "γνωμοδότηση",
         "Α.5": "πρακτικό", "Α.6": "αιτιολογική έκθεση"}
ATOMIKI = re.compile(r"κατ.? εξαιρεσ\w* ταξινομ|εντολη πληρωμ|αναληψη υποχρεωσ|μετακινησ|εκκαθαριση|"
                     r"πληρωμη|δαπαν|ορισμος|συγκροτηση")


def xtonos(s):
    s = unicodedata.normalize("NFD", s or "")
    return "".join(c for c in s if unicodedata.category(c) != "Mn").lower().replace("ς", "σ")


def thema(s):
    return bool(THEMA.search(xtonos(s)))


def fere(url, prospatheies=4, timeout=40):
    """curl, όχι urllib: το yme.gov.gr απαντά 403 στο urllib (16/11/2026)· επανάληψη σε κομμένη σύνδεση."""
    teleftaio = None
    for i in range(prospatheies):
        r = subprocess.run(["curl", "-sS", "-L", "--compressed", "-m", str(timeout), "-A", UA["User-Agent"],
                            "-w", "\n%{http_code} %{url_effective}", url], capture_output=True)
        if r.returncode == 0:
            soma, _, tel = r.stdout.rpartition(b"\n")
            kod, _, telikο = tel.decode().partition(" ")
            if kod.startswith("2"):
                return soma, telikο
            teleftaio = f"HTTP {kod}"
            if kod in ("403", "404", "429"):
                break
        else:
            teleftaio = r.stderr.decode("utf-8", "replace").strip()
        time.sleep(1.5 * (i + 1))
    raise RuntimeError(f"{url}: {teleftaio}")


def keimeno_html(s):
    s = re.sub(r"<(script|style|noscript)\b.*?</\1>", " ", s, flags=re.S | re.I)
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", s))).strip()


def h(s):
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]


# ---------------------------------------------------------------- A. Δελτία Τύπου ΥΜΕ
def pigi_A(kat, eur):
    raw, _ = fere(YME_DT_RSS)
    root = ET.fromstring(raw)
    eidomena = set(kat.setdefault("A_eidomena", []))
    nea = 0
    for it in root.findall(".//item"):
        link = (it.findtext("link") or "").strip()
        titlos = (it.findtext("title") or "").strip()
        perig = keimeno_html(it.findtext("description") or "")
        imer = it.findtext("pubDate") or ""
        if not link or h(link)[:8] in eidomena:
            continue
        eidomena.add(h(link)[:8])
        nea += 1
        if kat.get("A_proti", True):  # πρώτη εκτέλεση: «νέο» = τελευταίες 10 ημέρες
            try:
                if parsedate_to_datetime(imer).date() < dt.date.today() - dt.timedelta(days=10):
                    continue
            except Exception:
                continue
        sxetiko = thema(titlos + " " + perig[:1500])
        eur.append({"pigi": "ΥΜΕ — Δελτία Τύπου", "eidos": "νέο δελτίο τύπου",
                    "sxetiko": sxetiko, "titlos": titlos, "link": link,
                    "imerominia": imer, "perilipsi": perig[:300]})
    kat["A_eidomena"] = sorted(eidomena)[-500:]
    proti = kat.get("A_proti", True)
    kat["A_proti"] = False
    return {"egrafes_rss": len(root.findall('.//item')), "nees": nea, "grammi_vasis": proti}


# ---------------------------------------------------------------- B. Σελίδες ΥΜΕ
SYNIMMENO = re.compile(r"\.(pdf|docx?|xlsx?|xls|zip|rar|odt|ods)(\?|$)", re.I)


DOWNLOAD = re.compile(r"/item/download/\d+_[0-9a-f]+")
MINES = {m: i + 1 for i, m in enumerate(["Ιανουαρίου", "Φεβρουαρίου", "Μαρτίου", "Απριλίου", "Μαΐου", "Ιουνίου",
                                         "Ιουλίου", "Αυγούστου", "Σεπτεμβρίου", "Οκτωβρίου", "Νοεμβρίου", "Δεκεμβρίου"])}


def kanoniko(path):
    return urllib.parse.unquote(html.unescape(path)).replace(YME, "").split("#")[0].rstrip("/")


def anagnosi_selidas(path):
    raw, _ = fere(YME + urllib.parse.quote(path, safe="/-_.~%"))
    s = raw.decode("utf-8", "replace")
    m = re.search(r"<!-- Start K2 (Item|Category) Layout -->(.*)<!-- End K2 \1 Layout -->", s, re.S)
    soma_html = m.group(2) if m else s
    # Συνημμένα: σύνδεσμοι K2 /item/download/<αρ>_<hash> (με το όνομα αρχείου ως κείμενο) ή αρχεία με κατάληξη
    synim = {}
    for href, onoma in re.findall(r'<a [^>]*href="([^"]+)"[^>]*>(.*?)</a>', soma_html, re.S):
        hr = html.unescape(href)
        if DOWNLOAD.search(hr) or SYNIMMENO.search(hr):
            synim[kanoniko(hr) if hr.startswith("/") or hr.startswith(YME) else hr] = keimeno_html(onoma)[:200]
    paidia = set()
    for href in re.findall(r'href="([^"#]+)"', s):
        l = kanoniko(href)
        if l.startswith("/") and "?" not in l and not DOWNLOAD.search(l) and not SYNIMMENO.search(l) \
                and any(l.startswith(e) for e in YME_ENOTITES):
            paidia.add(l)
    tropop = ""
    mt = re.search(r'class="itemDateModified">(.{0,400})', soma_html, re.S)
    if mt:
        t = keimeno_html(mt.group(1))
        md = re.search(r"(\d{1,2}) (\w+) (\d{4}) (\d{1,2}:\d{2})", t)
        tropop = f"{md.group(3)}-{MINES.get(md.group(2), 0):02d}-{int(md.group(1)):02d} {md.group(4)}" if md else t
    # Για σύγκριση μόνο τίτλος + σώμα άρθρου: έξω τα «Τελευταία του συντάκτη» (itemAuthorLatest) και
    # «More in this category» (itemNavigation), που αλλάζουν όταν ανεβαίνει οτιδήποτε αλλού (16/11/2026).
    i = soma_html.find('class="itemBody"')
    if i >= 0:
        tel = [j for j in (soma_html.find(x, i) for x in ('class="itemLinks"', 'class="itemAuthorLatest"',
               'class="itemNavigation"', 'class="itemContentFooter"', 'class="itemRelated"')) if j > 0]
        th = re.search(r'class="itemTitle"(.*?)</h', soma_html, re.S)
        kath = (th.group(1) if th else "") + soma_html[i:min(tel) if tel else len(soma_html)]
    else:
        kath = re.sub(r'<div class="(itemAuthorLatest|itemNavigation|itemRelated)".*?</div>\s*</div>', " ",
                      soma_html, flags=re.S)
    kath = re.sub(r'<(div|span) class="(itemHits|catItemHits|itemRatingBlock|itemDateModified)".*?</\1>', " ",
                  kath, flags=re.S)
    kath = re.sub(r'cloak[0-9a-f]{32}', 'cloak', kath)
    soma = keimeno_html(kath)
    tit = re.search(r"<title>(.*?)</title>", s, re.S)
    return {"titlos": keimeno_html(tit.group(1)) if tit else path, "hash": h(soma), "synimmena": synim,
            "tropopoiisi": tropop, "paidia": sorted(paidia)}


def kleidi_synimmenou(a):
    m = re.search(r"/item/download/(\d+)_", a)
    return m.group(1) if m else "h" + h(a)[:6]


def epivevaiosi(p, pal_hash, palia_synim, fores=2):
    """Η αλλαγή μετρά μόνο αν η σελίδα, ξαναδιαβασμένη, διαφέρει ξανά από την παλιά εικόνα (16/11/2026:
    το yme.gov.gr έδωσε μία φορά διαφορετική απόκριση για αμετάβλητη σελίδα)."""
    for _ in range(fores):
        time.sleep(2)
        try:
            r2 = anagnosi_selidas(p)
        except Exception:
            return False
        if r2["hash"][:8] == pal_hash and \
                {kleidi_synimmenou(x) for x in r2["synimmena"]} <= palia_synim:
            return False
    return True


def pigi_B(kat, eur):
    """Κατάσταση (συμπαγής, ASCII): B_s = {h(διαδρομή)[:8]: [hash κειμένου[:8], Τελευταία Τροποποίηση]},
    B_a = ταξινομημένη λίστα κλειδιών συνημμένων (αριθμός λήψης K2 ή h+6)."""
    pal_s = kat.get("B_s", {})
    pal_a = set(kat.get("B_a", []))
    proti = not pal_s
    oura, eidomena, selides, sfalmata = list(YME_ENOTITES), set(), {}, []
    with cf.ThreadPoolExecutor(max_workers=3) as ex:
        while oura and len(eidomena) < YME_MAX_SELIDES:
            partida = []
            while oura and len(partida) < 15:
                p = oura.pop(0)
                if p not in eidomena:
                    eidomena.add(p)
                    partida.append(p)
            mel = {ex.submit(anagnosi_selidas, p): p for p in partida}
            for f in cf.as_completed(mel):
                p = mel[f]
                try:
                    r = f.result()
                except Exception as e:
                    sfalmata.append(f"{p}: {e}")
                    continue
                selides[p] = r
                for c in r["paidia"]:
                    if c not in eidomena:
                        oura.append(c)
            time.sleep(0.3)
    nea_s, nea_a = {}, set()
    apotyximenes = {h(x.split(":")[0])[:8] for x in sfalmata}
    for k in apotyximenes & set(pal_s):  # σελίδα που δεν διαβάστηκε: κράτα την παλιά εικόνα
        nea_s[k] = pal_s[k]
    for p, r in sorted(selides.items(), key=lambda x: x[1].get("tropopoiisi", ""), reverse=True):
        k = h(p)[:8]
        url = YME + urllib.parse.quote(p, safe="/-_.~")
        klid = {kleidi_synimmenou(a): a for a in r["synimmena"]}
        nea_s[k] = [r["hash"][:8], r["tropopoiisi"]]
        nea_a |= set(klid)
        if proti:  # πρώτη εκτέλεση: «νέο» = Τελευταία Τροποποίηση στις τελευταίες 30 ημέρες
            if r["tropopoiisi"][:10] >= (dt.date.today() - dt.timedelta(days=30)).isoformat():
                eur.append({"pigi": "ΥΜΕ — ενότητες οχημάτων", "eidos": "πρόσφατη τροποποίηση σελίδας",
                            "sxetiko": True, "titlos": r["titlos"], "link": url, "imerominia": r["tropopoiisi"]})
            continue
        pal = pal_s.get(k)
        if pal is None:
            eur.append({"pigi": "ΥΜΕ — ενότητες οχημάτων", "eidos": "νέα σελίδα", "sxetiko": True,
                        "titlos": r["titlos"], "link": url, "imerominia": r["tropopoiisi"]})
            continue
        prost = [klid[x] for x in sorted(set(klid) - pal_a)]
        if (prost or r["hash"][:8] != pal[0]) and not epivevaiosi(p, pal[0], pal_a):
            nea_s[k] = pal
            nea_a -= set(klid) - pal_a
            continue
        for a in prost:
            eur.append({"pigi": "ΥΜΕ — ενότητες οχημάτων", "eidos": "νέο συνημμένο", "sxetiko": True,
                        "titlos": f"{r['titlos']}: {r['synimmena'][a] or Path(a).name}",
                        "link": a if a.startswith("http") else YME + urllib.parse.quote(a, safe="/-_.~"),
                        "selida": url, "imerominia": r["tropopoiisi"]})
        if r["hash"][:8] != pal[0] and not prost:
            eur.append({"pigi": "ΥΜΕ — ενότητες οχημάτων", "eidos": "αλλαγή κειμένου σελίδας",
                        "sxetiko": True, "titlos": r["titlos"], "link": url, "imerominia": r["tropopoiisi"],
                        "perilipsi": f"Τελευταία Τροποποίηση: {pal[1] or '—'} → {r['tropopoiisi'] or '—'}"})
    plires = not sfalmata and len(eidomena) < YME_MAX_SELIDES
    if not proti and plires:
        xamenes = set(pal_s) - set(nea_s)
        xamena = pal_a - nea_a
        if xamenes or xamena:
            eur.append({"pigi": "ΥΜΕ — ενότητες οχημάτων", "eidos": "αφαιρέθηκαν σελίδες ή συνημμένα",
                        "sxetiko": True, "titlos": f"{len(xamenes)} σελίδες και {len(xamena)} συνημμένα δεν "
                        f"εμφανίζονται πια (κλειδιά συνημμένων: {', '.join(sorted(xamena)[:15])})",
                        "link": YME + YME_ENOTITES[0]})
    if not plires:  # ελλιπής ανίχνευση: μην «ξεχάσεις» ό,τι δεν είδες
        for k, v in pal_s.items():
            nea_s.setdefault(k, v)
        nea_a |= pal_a
    kat["B_s"], kat["B_a"] = nea_s, sorted(nea_a)
    kat.pop("B_selides", None)
    return {"selides": len(selides), "synimmena": len(nea_a), "sfalmata": sfalmata[:10],
            "plithos_sfalmaton": len(sfalmata), "grammi_vasis": proti, "orio": len(eidomena) >= YME_MAX_SELIDES}


# ---------------------------------------------------------------- C. Διαύγεια ΥΠΟΜΕ
def pigi_C(kat, eur, apo):
    eos = dt.date.today()
    if apo is None:
        apo = dt.date.fromisoformat(kat["C_teleftaio"]) - dt.timedelta(days=2) if kat.get("C_teleftaio") \
            else eos - dt.timedelta(days=10)
    eidomena = set(kat.setdefault("C_eidomena", []))
    sel, synolo, sarothikan, sxetika = 0, None, 0, 0
    while True:
        q = urllib.parse.urlencode({"org": YPOME, "from_issue_date": apo.isoformat(),
                                    "to_issue_date": eos.isoformat(), "size": 500, "page": sel})
        raw, _ = fere(f"{DIAVGEIA}?{q}")
        d = json.loads(raw)
        synolo = d.get("info", {}).get("total", 0)
        apof = d.get("decisions", [])
        for a in apof:
            sarothikan += 1
            ada, thema_ = a.get("ada"), a.get("subject") or ""
            typos = a.get("decisionTypeId", "")
            if not ada or ada in eidomena:
                continue
            if thema(thema_) and typos.startswith("Α."):
                eidomena.add(ada)  # μόνο κανονιστικές/εγκύκλιοι/ατομικές διοικητικές
                sxetika += 1
                atom = bool(ATOMIKI.search(xtonos(thema_)))
                eur.append({"pigi": "Διαύγεια — ΥΠΟΜΕ", "eidos": TYPOI.get(typos, typos),
                            "sxetiko": not atom, "atomiki": atom,
                            "titlos": thema_.strip()[:300], "link": f"https://diavgeia.gov.gr/decision/view/{ada}",
                            "ada": ada, "imerominia": dt.datetime.fromtimestamp(
                                a.get("issueDate", 0) / 1000, ATHINA).date().isoformat()})
        sel += 1
        if not apof or sel * 500 >= synolo:
            break
        time.sleep(0.5)
    kat["C_eidomena"] = sorted(eidomena)[-1500:]
    kat["C_teleftaio"] = eos.isoformat()
    return {"apo": apo.isoformat(), "eos": eos.isoformat(), "synolo": synolo, "sarothikan": sarothikan,
            "thematika": sxetika, "plires": sarothikan >= (synolo or 0)}


# ---------------------------------------------------------------- D. Εθνικό Μητρώο
def mitos_titlos(kod):
    q = urllib.parse.urlencode({"search": kod, "fulltext": 1})
    raw, _ = fere(f"{MITOS}?{q}")
    s = raw.decode("utf-8", "replace")
    titloi = re.findall(r'mw-search-result-heading"><a href="([^"]+)" title="([^"]+)"', s)
    sfalma = None
    for href, t in [x for x in titloi if html.unescape(x[1]).startswith("ΔΔ:")][:5]:
        try:
            sel = mitos_selida(href)
        except Exception as e:
            sfalma = e
            continue
        if sel["kodikos"] == kod:  # επιβεβαίωση: ο κωδικός της σελίδας (μετά το UUID) είναι αυτός
            return href, html.unescape(t), sel
    if sfalma:
        raise RuntimeError(f"ανάγνωση σελίδας: {sfalma}")
    return None, None, None


def mitos_selida(href):
    raw, _ = fere("https://mitos.gov.gr" + urllib.parse.quote(urllib.parse.unquote(href), safe="/:_(),.-"))
    s = raw.decode("utf-8", "replace")
    m = re.search(r'id="mw-content-text"(.*?)<div class="printfooter"', s, re.S)
    k = keimeno_html(m.group(1) if m else s)
    imer = re.search(r"Τελευταία ενημέρωση\s*(\d{2}/\d{2}/\d{4})", k)
    ousia = re.sub(r"Τελευταία ενημέρωση\s*\d{2}/\d{2}/\d{4}", "", k)
    kd = re.search(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\s+(\d{6})\b", k[:600])
    return {"keimeno": k, "imerominia": imer.group(1) if imer else "", "hash": h(ousia),
            "kodikos": kd.group(1) if kd else ""}


def ena_mitos(kod, pal):
    time.sleep(0.4)
    href, t, sel = mitos_titlos(kod)
    return kod, href, t, sel


def pigi_D(kat, eur, lista):
    kodikoi = []
    for gr in Path(lista).read_text(encoding="utf-8").splitlines():
        if gr and not gr.startswith("#"):
            p = gr.split("\t")
            kodikoi.append((p[0].strip(), p[1].strip() if len(p) > 1 else ""))
    proig = {k: v for k, v in kat.get("D_diadikasies", {}).items() if isinstance(v, list)}
    proti = not proig
    nea, sfalmata = {}, []
    with cf.ThreadPoolExecutor(max_workers=2) as ex:
        mel = {ex.submit(ena_mitos, k, proig.get(k)): (k, o) for k, o in kodikoi}
        for f in cf.as_completed(mel):
            k, o = mel[f]
            try:
                _, href, t, sel = f.result()
            except Exception as e:
                sfalmata.append(f"{k}: {e}")
                if k in proig:
                    nea[k] = proig[k]
                continue
            if not sel:
                sfalmata.append(f"{k}: δεν βρέθηκε σελίδα με τον κωδικό")
                if k in proig:
                    nea[k] = proig[k]
                    if not proti:
                        eur.append({"pigi": "Εθνικό Μητρώο Διοικητικών Διαδικασιών", "eidos": "η διαδικασία δεν βρέθηκε",
                                    "sxetiko": True, "titlos": f"{k} (δεν βρέθηκε σελίδα με αυτόν τον κωδικό)",
                                    "link": f"https://mitos.gov.gr/index.php?search={k}&fulltext=1"})
                continue
            nea[k] = [sel["hash"][:8], sel["imerominia"]]
            pal = proig.get(k)
            if not proti and pal and pal[0] != sel["hash"][:8]:
                eur.append({"pigi": "Εθνικό Μητρώο Διοικητικών Διαδικασιών", "eidos": "αλλαγή ουσίας διαδικασίας",
                            "sxetiko": True, "titlos": f"{k} {t.replace('ΔΔ:', '')}",
                            "link": "https://mitos.gov.gr" + urllib.parse.quote(urllib.parse.unquote(href), safe="/:_(),.-"),
                            "imerominia": sel["imerominia"], "proigoumeni_enimerosi": pal[1]})
    kat["D_diadikasies"] = nea
    return {"lista": len(kodikoi), "diavastikan": len(nea) - len([s for s in sfalmata if s.split(':')[0] in proig]),
            "nea_imerominia_enimerosis": sum(1 for k, v in nea.items() if proig.get(k) and v[1] != proig[k][1]),
            "sfalmata": sfalmata[:15], "plithos_sfalmaton": len(sfalmata), "grammi_vasis": proti}


# ---------------------------------------------------------------- Έξοδος
def roi_rss(palies, nees, arxeio):
    tora = dt.datetime.now(ATHINA)
    for e in nees:
        e.setdefault("katagrafi", tora.isoformat(timespec="seconds"))
    ola = (nees + palies)[:200]
    rss = ET.Element("rss", version="2.0")
    ch = ET.SubElement(rss, "channel")
    ET.SubElement(ch, "title").text = "Τεχνικό Γραφείο Μουδιώτης — Ενημερώσεις επίσημων πηγών"
    ET.SubElement(ch, "link").text = ROI_URL.rsplit("/", 1)[0] + "/"
    ET.SubElement(ch, "description").text = ("ΥΜΕ (Δελτία Τύπου, ενότητες οχημάτων), Διαύγεια ΥΠΟΜΕ, Εθνικό Μητρώο "
                                             "Διοικητικών Διαδικασιών — μόνο θεματικά σχετικά ευρήματα")
    ET.SubElement(ch, "language").text = "el"
    ET.SubElement(ch, "lastBuildDate").text = format_datetime(tora)
    for e in ola:
        it = ET.SubElement(ch, "item")
        ET.SubElement(it, "title").text = f"[{e['pigi']} · {e['eidos']}] {e['titlos']}"
        ET.SubElement(it, "link").text = e["link"]
        ET.SubElement(it, "guid", isPermaLink="false").text = h(e["link"] + e["eidos"] + e.get("imerominia", "") +
                                                                 e.get("katagrafi", "")[:10])
        ET.SubElement(it, "pubDate").text = format_datetime(dt.datetime.fromisoformat(e["katagrafi"]))
        ET.SubElement(it, "category").text = e["pigi"]
        per = e.get("perilipsi") or ""
        if e.get("selida"):
            per += f" Σελίδα: {e['selida']}"
        if e.get("proigoumeni_enimerosi"):
            per += f" Ενημέρωση Μητρώου: {e['proigoumeni_enimerosi']} → {e.get('imerominia')}"
        ET.SubElement(it, "description").text = per.strip() or e["titlos"]
    ET.indent(rss)
    Path(arxeio).write_bytes(b'<?xml version="1.0" encoding="utf-8"?>\n' + ET.tostring(rss, encoding="utf-8"))
    return ola


def opml(arxeio):
    Path(arxeio).write_text(f"""<?xml version="1.0" encoding="utf-8"?>
<opml version="2.0">
  <head><title>Τεχνικό Γραφείο Μουδιώτης — ροές</title><dateCreated>{format_datetime(dt.datetime.now(ATHINA))}</dateCreated></head>
  <body>
    <outline text="Επίσημες πηγές" title="Επίσημες πηγές">
      <outline type="rss" text="Τεχνικό Γραφείο Μουδιώτης — Ενημερώσεις επίσημων πηγών" title="Τεχνικό Γραφείο Μουδιώτης — Ενημερώσεις επίσημων πηγών" xmlUrl="{ROI_URL}" htmlUrl="{ROI_URL.rsplit('/', 1)[0]}/"/>
      <outline type="rss" text="ΥΜΕ — Δελτία Τύπου" title="ΥΜΕ — Δελτία Τύπου" xmlUrl="{html.escape(YME_DT_RSS)}" htmlUrl="{YME}/2013-01-31-06-37-23/2013-01-31-07-00-49"/>
    </outline>
  </body>
</opml>
""", encoding="utf-8")


def fortosi(keimeno):
    """Η κατάσταση στον δίσκο είναι πυκνή (λίγες γραμμές, < 25 χιλ. tokens): B_a σε ομάδες των 30 χωρισμένες με
    κενό, B_s και D_diadikasies ως «αποτύπωμα|ημερομηνία»."""
    k = json.loads(keimeno) if keimeno.strip() else {}
    k["B_a"] = [x for gr in k.get("B_a", []) for x in str(gr).split()]
    for kl in ("B_s", "D_diadikasies"):
        k[kl] = {a: (v.split("|", 1) if isinstance(v, str) else v) for a, v in k.get(kl, {}).items()}
    return k


def apothikefsi(kat):
    k = dict(kat)
    a = k.get("B_a", [])
    k["B_a"] = [" ".join(a[i:i + 30]) for i in range(0, len(a), 30)]
    for kl in ("B_s", "D_diadikasies"):
        k[kl] = {x: "|".join(v) for x, v in k.get(kl, {}).items()}
    return json.dumps(k, ensure_ascii=False, indent=0)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--katastasi", required=True)
    ap.add_argument("--lista", required=True)
    ap.add_argument("--exodos", required=True)
    ap.add_argument("--apo")
    ap.add_argument("--mono", default="A,B,C,D")
    a = ap.parse_args()
    kp = Path(a.katastasi)
    kat = fortosi(kp.read_text(encoding="utf-8")) if kp.exists() and kp.stat().st_size else {}
    out = Path(a.exodos); out.mkdir(parents=True, exist_ok=True)
    eur, stoixeia, apotyxies = [], {}, []
    for gramma, f in (("A", lambda: pigi_A(kat, eur)), ("B", lambda: pigi_B(kat, eur)),
                      ("C", lambda: pigi_C(kat, eur, dt.date.fromisoformat(a.apo) if a.apo else None)),
                      ("D", lambda: pigi_D(kat, eur, a.lista))):
        if gramma not in a.mono.split(","):
            continue
        t0 = time.time()
        try:
            stoixeia[gramma] = f()
            stoixeia[gramma]["deuterolepta"] = round(time.time() - t0)
            s = stoixeia[gramma]
            if s.get("plithos_sfalmaton") or s.get("plires") is False or s.get("orio"):
                apotyxies.append(gramma)
        except Exception as e:
            stoixeia[gramma] = {"sfalma": str(e)}
            apotyxies.append(gramma)
    sxetika = [e for e in eur if e.get("sxetiko")]
    palies = kat.get("roi", [])
    kat["roi"] = roi_rss(palies, sxetika, out / "roi.xml")[:30]
    opml(out / "piges.opml")
    kat["teleftaios_elegxos"] = dt.datetime.now(ATHINA).isoformat(timespec="seconds")
    kp.write_text(apothikefsi(kat), encoding="utf-8")
    (out / "eurimata.json").write_text(json.dumps({"stoixeia": stoixeia, "eurimata": eur,
                                                   "apotyxies": apotyxies}, ensure_ascii=False, indent=1),
                                       encoding="utf-8")
    onomata = {"A": "ΥΜΕ Δελτία Τύπου", "B": "ΥΜΕ ενότητες οχημάτων", "C": "Διαύγεια ΥΠΟΜΕ",
               "D": "Εθνικό Μητρώο Διοικητικών Διαδικασιών"}
    gr = [f"# Έλεγχος επίσημων πηγών — {kat['teleftaios_elegxos'][:16].replace('T', ' ')}", "",
          "## Σχετικά ευρήματα", ""]
    gr += [f"- **{e['pigi']}** · {e['eidos']}: {e['titlos']} — {e['link']}" for e in sxetika] or ["- Κανένα."]
    atom = [e for e in eur if e.get("atomiki")]
    if atom:
        gr += ["", "## Ατομικές πράξεις με θεματικό όρο (χαμηλή σημασία)", ""]
        gr += [f"- {e['titlos']} — {e['link']}" for e in atom]
    gr += ["", "## Τι ελέγχθηκε", ""]
    gr += [f"- {onomata[k]}: {json.dumps(v, ensure_ascii=False)}" for k, v in stoixeia.items()]
    if apotyxies:
        gr += ["", f"**Ελλιπής έλεγχος:** {', '.join(onomata[x] for x in apotyxies)}"]
    (out / "anafora.md").write_text("\n".join(gr) + "\n", encoding="utf-8")
    print("\n".join(gr))
    return 3 if apotyxies else 0


if __name__ == "__main__":
    sys.exit(main())
