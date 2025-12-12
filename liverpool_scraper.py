import os
import sys
import time
import json
import random
from datetime import datetime
from typing import List, Dict, Any, Optional

import requests
import pandas as pd
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ============================================================
# 1) CÓDIGOS DE LIVERPOOL (uno por línea, puedes comentar con #)
# ============================================================
INPUT_CODES = """
1086327259
1086327321

""".strip()

# Si algún día quieres pasar URLs PDP directas, puedes llenarlo:
INPUT_URLS: List[str] = [
    # "https://www.liverpool.com.mx/tienda/pdp/lo-que-sea/1175413363",
]

# URL base de PDP
PDP_URL_TEMPLATE = "https://www.liverpool.com.mx/tienda/pdp/lo-que-sea/{code}"

# ============================================================
# 2) Parámetros de tiempos / loops (OPTIMIZADOS)
# ============================================================

DEFAULT_TIME_BUDGET_SECONDS = 5400  # 1.5 h por loop
MAX_LOOPS_DEFAULT = 3

INITIAL_WAIT_RANGE = (3.0, 6.0)
BETWEEN_REQUESTS = (2.0, 4.0)

MAX_RETRIES = 7
BACKOFF_BASE = 4.0
BACKOFF_CAP = 90.0

ROLLING_WINDOW = 6
ALPHA_SENSITIVITY = 1.2

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36"
)

# ============================================================
# 3) Sesión HTTP con retry básico (5xx)
# ============================================================
session = requests.Session()
session.headers.update({
    "User-Agent": UA,
    "Accept-Language": "es-MX,es;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.liverpool.com.mx/",
    "Cache-Control": "no-cache",
})
retry = Retry(
    total=MAX_RETRIES,
    connect=MAX_RETRIES,
    read=MAX_RETRIES,
    backoff_factor=0.5,
    status_forcelist=[500, 502, 503, 504],
    allowed_methods=["GET", "HEAD"],
    raise_on_status=False,
)
session.mount("http://", HTTPAdapter(max_retries=retry))
session.mount("https://", HTTPAdapter(max_retries=retry))

# ============================================================
# 4) Helpers de tiempos y backoff
# ============================================================
def jitter(a: float, b: float) -> float:
    return random.uniform(a, b)

def sleep_range(a: float, b: float) -> float:
    t = jitter(a, b)
    time.sleep(t)
    return t

recent_429: List[bool] = []

def current_429_ratio() -> float:
    if not recent_429:
        return 0.0
    return sum(1 for x in recent_429 if x) / len(recent_429)

def planned_initial_wait() -> float:
    base = jitter(*INITIAL_WAIT_RANGE)
    ratio = current_429_ratio()
    multiplier = 1.0 + ALPHA_SENSITIVITY * ratio
    return base * multiplier

def get_with_backoff(
    url: str,
    allow_redirects: bool = True,
    timeout: int = 40,
    mark_429_flag: Optional[list] = None
) -> Optional[requests.Response]:
    last_status = None
    for i in range(MAX_RETRIES):
        try:
            r = session.get(url, allow_redirects=allow_redirects, timeout=timeout)
            last_status = r.status_code

            if r.status_code in (200, 404):
                return r

            if r.status_code in (429, 403):
                if mark_429_flag is not None:
                    mark_429_flag[0] = True
                wait = min(BACKOFF_BASE * (2 ** i), BACKOFF_CAP) + jitter(1.0, 4.0)
                print(f"   HTTP {r.status_code} en {url} -> backoff {wait:.1f}s (reintento {i+1}/{MAX_RETRIES})")
                time.sleep(wait)
                continue

            time.sleep(1.5 + i * 0.5)

        except requests.RequestException as e:
            wait = 2.0 + i * 1.25
            print(f"   Error de red en {url}: {e} -> esperando {wait:.1f}s (reintento {i+1}/{MAX_RETRIES})")
            time.sleep(wait)

    print(f"   ❌ No se pudo obtener {url} tras {MAX_RETRIES} intentos (último status={last_status})")
    return None

# ============================================================
# 5) Parseo desde __NEXT_DATA__
# ============================================================
def parse_product_from_html(html: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html, "lxml")

    script_tag = soup.find("script", id="__NEXT_DATA__")
    if not script_tag:
        print("⚠️ No encontré <script id='__NEXT_DATA__'> en el HTML.")
        return {}

    try:
        data = json.loads(script_tag.string)
    except Exception as e:
        print("⚠️ Error al parsear JSON de __NEXT_DATA__:", e)
        return {}

    try:
        records = data["query"]["data"]["mainContent"]["records"]
        if not records:
            raise KeyError("records vacío")
        rec0 = records[0]
    except Exception as e:
        print("⚠️ No encontré records[0] en __NEXT_DATA__:", e)
        return {}

    all_meta = rec0.get("allMeta") or {}
    variants = all_meta.get("variants") or []
    variant0 = variants[0] if variants else {}

    title = (
        all_meta.get("TituloSinMarca")
        or all_meta.get("productDisplayName")
        or all_meta.get("productName")
        or all_meta.get("productTitle")
        or variant0.get("skuName")
        or rec0.get("_t")
        or ""
    )

    code = (
        variant0.get("skuId")
        or variant0.get("sellerSkuId")
        or all_meta.get("productId")
        or ""
    )

    prices_variant = variant0.get("prices") or {}
    list_price = (
        prices_variant.get("listPrice")
        or prices_variant.get("regularPrice")
        or prices_variant.get("basePrice")
    )
    discount_price = (
        prices_variant.get("promoPrice")
        or prices_variant.get("salePrice")
        or prices_variant.get("sortPrice")
        or prices_variant.get("offerPrice")
    )

    if list_price is None:
        list_price = (
            all_meta.get("listPrice")
            or all_meta.get("regularPrice")
            or all_meta.get("basePrice")
        )

    if discount_price is None:
        discount_price = (
            all_meta.get("promoPrice")
            or all_meta.get("salePrice")
            or all_meta.get("sortPrice")
            or all_meta.get("offerPrice")
        )

    offers = variant0.get("offers") or {}
    best_offer = offers.get("bestOffer") or {}

    seller = best_offer.get("sellerName")
    if not seller:
        sellernames = variant0.get("sellernames")
        if isinstance(sellernames, list) and sellernames:
            seller = sellernames[0]
        else:
            seller = ""

    def to_float(x):
        if x is None:
            return None
        try:
            return float(x)
        except Exception:
            return None

    return {
        "CODIGO_PRODUCTO": code,
        "TITULO": title,
        "PRECIO_REGULAR_NUM": to_float(list_price),
        "PRECIO_DESCUENTO_NUM": to_float(discount_price),
        "VENDEDOR": seller,
    }

# ============================================================
# 6) Helpers de salida
# ============================================================
COLUMNS = [
    "TIMESTAMP",
    "SKU",
    "URL_PDP",
    "CODIGO_PRODUCTO",
    "TITULO",
    "PRECIO_REGULAR_NUM",
    "PRECIO_DESCUENTO_NUM",
    "VENDEDOR",
    "STATUS",
]

def row_to_tsv(row: Dict[str, Any]) -> str:
    def fmt(x):
        if x is None:
            return ""
        s = str(x)
        return s.replace("\t", " ").replace("\r", " ").replace("\n", " ").strip()
    return "\t".join(fmt(row.get(col, "")) for col in COLUMNS)

def print_header_once():
    print("\t".join(COLUMNS))
    sys.stdout.flush()

def save_results(rows: List[Dict[str, Any]]):
    if not rows:
        print("⚠️ No hay resultados para guardar todavía.")
        return

    df = pd.DataFrame(rows, columns=COLUMNS)
    df.to_csv("liverpool_datos.csv", index=False, encoding="utf-8-sig")

    with pd.ExcelWriter("liverpool_datos.xlsx", engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Datos")
        ws = writer.sheets["Datos"]
        col = "URL_PDP"
        c = df.columns.get_loc(col)
        for r, val in enumerate(df[col].fillna(""), start=1):
            if isinstance(val, str) and val.startswith("http"):
                ws.write_url(r, c, val, string=val)

    print("💾 Guardados 'liverpool_datos.csv' y 'liverpool_datos.xlsx'.")

# ============================================================
# 7) Procesar un código o una URL
# ============================================================
def process_code(code: str) -> Dict[str, Any]:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sku = code
    url_pdp = PDP_URL_TEMPLATE.format(code=code)
    status = "OK"

    saw_429 = [False]

    initial_wait = planned_initial_wait()
    print(f"   ⏳ Espera inicial antes de abrir '{sku}': {initial_wait:.1f}s (ratio 429 reciente: {current_429_ratio():.2f})")
    time.sleep(initial_wait)

    r = get_with_backoff(url_pdp, mark_429_flag=saw_429)
    if not r:
        status = "HTTP error PDP"
        row = {
            "TIMESTAMP": ts,
            "SKU": sku,
            "URL_PDP": url_pdp,
            "CODIGO_PRODUCTO": "",
            "TITULO": "",
            "PRECIO_REGULAR_NUM": "",
            "PRECIO_DESCUENTO_NUM": "",
            "VENDEDOR": "",
            "STATUS": status,
        }
    elif r.status_code == 404:
        status = "404 PDP"
        row = {
            "TIMESTAMP": ts,
            "SKU": sku,
            "URL_PDP": url_pdp,
            "CODIGO_PRODUCTO": "",
            "TITULO": "",
            "PRECIO_REGULAR_NUM": "",
            "PRECIO_DESCUENTO_NUM": "",
            "VENDEDOR": "",
            "STATUS": status,
        }
    else:
        info = parse_product_from_html(r.text)
        if not info:
            status = "Formato PDP desconocido"
            row = {
                "TIMESTAMP": ts,
                "SKU": sku,
                "URL_PDP": url_pdp,
                "CODIGO_PRODUCTO": "",
                "TITULO": "",
                "PRECIO_REGULAR_NUM": "",
                "PRECIO_DESCUENTO_NUM": "",
                "VENDEDOR": "",
                "STATUS": status,
            }
        else:
            row = {
                "TIMESTAMP": ts,
                "SKU": sku,
                "URL_PDP": url_pdp,
                "CODIGO_PRODUCTO": info.get("CODIGO_PRODUCTO", ""),
                "TITULO": info.get("TITULO", ""),
                "PRECIO_REGULAR_NUM": info.get("PRECIO_REGULAR_NUM", ""),
                "PRECIO_DESCUENTO_NUM": info.get("PRECIO_DESCUENTO_NUM", ""),
                "VENDEDOR": info.get("VENDEDOR", ""),
                "STATUS": status,
            }

    recent_429.append(bool(saw_429[0]))
    if len(recent_429) > ROLLING_WINDOW:
        recent_429.pop(0)

    return row

def process_url(url: str) -> Dict[str, Any]:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    status = "OK"
    sku = ""

    saw_429 = [False]

    initial_wait = planned_initial_wait()
    print(f"   ⏳ Espera inicial antes de abrir URL directa: {initial_wait:.1f}s (ratio 429 reciente: {current_429_ratio():.2f})")
    time.sleep(initial_wait)

    r = get_with_backoff(url, mark_429_flag=saw_429)
    if not r:
        status = "HTTP error URL"
        row = {
            "TIMESTAMP": ts,
            "SKU": sku,
            "URL_PDP": url,
            "CODIGO_PRODUCTO": "",
            "TITULO": "",
            "PRECIO_REGULAR_NUM": "",
            "PRECIO_DESCUENTO_NUM": "",
            "VENDEDOR": "",
            "STATUS": status,
        }
    elif r.status_code == 404:
        status = "404 URL"
        row = {
            "TIMESTAMP": ts,
            "SKU": sku,
            "URL_PDP": url,
            "CODIGO_PRODUCTO": "",
            "TITULO": "",
            "PRECIO_REGULAR_NUM": "",
            "PRECIO_DESCUENTO_NUM": "",
            "VENDEDOR": "",
            "STATUS": status,
        }
    else:
        info = parse_product_from_html(r.text)
        if not info:
            status = "Formato PDP desconocido"
            row = {
                "TIMESTAMP": ts,
                "SKU": sku,
                "URL_PDP": url,
                "CODIGO_PRODUCTO": "",
                "TITULO": "",
                "PRECIO_REGULAR_NUM": "",
                "PRECIO_DESCUENTO_NUM": "",
                "VENDEDOR": "",
                "STATUS": status,
            }
        else:
            row = {
                "TIMESTAMP": ts,
                "SKU": sku,
                "URL_PDP": url,
                "CODIGO_PRODUCTO": info.get("CODIGO_PRODUCTO", ""),
                "TITULO": info.get("TITULO", ""),
                "PRECIO_REGULAR_NUM": info.get("PRECIO_REGULAR_NUM", ""),
                "PRECIO_DESCUENTO_NUM": info.get("PRECIO_DESCUENTO_NUM", ""),
                "VENDEDOR": info.get("VENDEDOR", ""),
                "STATUS": status,
            }

    recent_429.append(bool(saw_429[0]))
    if len(recent_429) > ROLLING_WINDOW:
        recent_429.pop(0)

    return row

# ============================================================
# 8) MAIN CON LOOPS Y GUARDADO POR LOOP
# ============================================================
def main() -> List[Dict[str, Any]]:
    codes = [
        ln.strip()
        for ln in INPUT_CODES.splitlines()
        if ln.strip() and not ln.strip().startswith("#")
    ]
    urls = [u.strip() for u in INPUT_URLS if u.strip()]

    items: List[tuple] = [("code", c) for c in codes] + [("url", u) for u in urls]
    total = len(items)

    print(f"Procesando {total} ítems de Liverpool…\n")
    print_header_once()

    time_budget = float(os.getenv("TIME_BUDGET_SECONDS", str(DEFAULT_TIME_BUDGET_SECONDS)))
    max_loops = int(os.getenv("MAX_LOOPS", str(MAX_LOOPS_DEFAULT)))

    all_results: List[Dict[str, Any]] = []
    pending_items = items

    for loop_idx in range(1, max_loops + 1):
        if not pending_items:
            print(f"\n✅ No hay pendientes para el loop {loop_idx}. Terminamos.")
            break

        print(f"\n================ LOOP {loop_idx}/{max_loops} - {len(pending_items)} ítems pendientes ================")
        loop_start = time.time()
        next_pending: List[tuple] = []

        for i, (kind, payload) in enumerate(pending_items, 1):
            elapsed = time.time() - loop_start
            if elapsed >= time_budget:
                print(f"\n⏰ Se alcanzó el límite de tiempo ({elapsed:.0f}s) en el loop {loop_idx}.")
                print("   Lo que falta se marcará como pendiente para el siguiente loop.")
                next_pending = pending_items[i-1:]
                break

            try:
                if kind == "code":
                    row = process_code(payload)
                else:
                    row = process_url(payload)

                all_results.append(row)
                print(f"[Loop {loop_idx}] [{i}/{len(pending_items)}] {payload} -> {row['STATUS']}")
                print(row_to_tsv(row))
                sys.stdout.flush()

            except Exception as e:
                ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                err_row = {
                    "TIMESTAMP": ts,
                    "SKU": payload if kind == "code" else "",
                    "URL_PDP": PDP_URL_TEMPLATE.format(code=payload) if kind == "code" else payload,
                    "CODIGO_PRODUCTO": "",
                    "TITULO": "",
                    "PRECIO_REGULAR_NUM": "",
                    "PRECIO_DESCUENTO_NUM": "",
                    "VENDEDOR": "",
                    "STATUS": f"Error: {e}",
                }
                all_results.append(err_row)
                print(f"[Loop {loop_idx}] [{i}/{len(pending_items)}] {payload} -> Error: {e}")
                print(row_to_tsv(err_row))
                sys.stdout.flush()

        save_results(all_results)
        pending_items = next_pending

    if pending_items:
        with open("liverpool_pendientes.txt", "w", encoding="utf-8") as f:
            for kind, payload in pending_items:
                f.write(payload + "\n")
        print(f"\n⚠️ Quedaron {len(pending_items)} ítems pendientes. Guardados en 'liverpool_pendientes.txt'.")
    else:
        print("\n✅ No quedaron pendientes.")

    print("\n🎉 Proceso terminado.")
    return all_results

# ============================================================
# 9) EMAIL (para GitHub: lee credenciales de variables de entorno)
# ============================================================
import smtplib
import ssl
from email.message import EmailMessage

def enviar_resultados_por_mail(
    sender: str,
    password: str,
    recipients: str,
    archivos_adjuntos=None,
    asunto: str = "Resultados scraper Liverpool"
):
    if archivos_adjuntos is None:
        archivos_adjuntos = []

    msg = EmailMessage()
    msg["Subject"] = asunto
    msg["From"] = sender
    msg["To"] = recipients

    cuerpo = (
        "Hola Abraham,\n\n"
        "Te mando los archivos generados por el scraper de Liverpool.\n\n"
        "Si ves varios correos en el mismo día, corresponden a diferentes ejecuciones del scraper.\n\n"
        "Saludos."
    )
    msg.set_content(cuerpo)

    for filename in archivos_adjuntos:
        try:
            with open(filename, "rb") as f:
                data = f.read()
            msg.add_attachment(
                data,
                maintype="application",
                subtype="octet-stream",
                filename=os.path.basename(filename),
            )
            print(f"✅ Adjuntado: {filename}")
        except FileNotFoundError:
            print(f"⚠️ No encontré el archivo: {filename}, no se adjunta.")

    context = ssl.create_default_context()
    print("📨 Enviando correo...")
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(sender, password)
        server.send_message(msg)
    print("✅ Correo enviado correctamente.")

# ============================================================
# 10) Punto de entrada para GitHub Actions
# ============================================================
def run():
    results = main()
    sender = os.environ.get("EMAIL_SENDER")
    password = os.environ.get("EMAIL_PASSWORD")
    recipients = os.environ.get("EMAIL_TO")

    if sender and password and recipients:
        enviar_resultados_por_mail(
            sender=sender,
            password=password,
            recipients=recipients,
            archivos_adjuntos=["liverpool_datos.csv", "liverpool_datos.xlsx"],
        )
    else:
        print("⚠️ EMAIL_SENDER / EMAIL_PASSWORD / EMAIL_TO no configuradas; no se envía correo.")

if __name__ == "__main__":
    run()
