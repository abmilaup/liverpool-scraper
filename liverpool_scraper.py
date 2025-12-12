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
1086306481
1107333734
1137477846
1096733824
1096730116
1141523071
1095621488
1096325271
1086081292
1086081322
1096255493
1096255825
1096255515
1096255949
1086081276
1153934491
1086980688
1099248145
1086081349
1086081268
1086980726
1086980637
1094820444
1101876094
1095561833
1095561841
1071105632
1086980777
1097040521
1099248153
1086081331
1086081306
1094290682
1096250602
1091236130
1086080229
1096716652
1086080261
1086080245
1086080237
1086980521
1108608001
1108608010
1096716679
1129904441
1102698271
1114100184
1095582067
1097040563
1108422765
1099248234
1149744599
1130860199
1094827171
1138965810
1096957773
1101876086
1145027205
1107023999
1108985883
1138556766
1108985930
1096959369
1151236894
1099810269
1099810251
1103609956
1096611648
1096611621
1099073736
1099073400
1102093867
1102094600
1102103251
1096079342
1086306146
1097029039
1097096526
1108422757
1151799058
1151799031
1151799023
1086306171
1087369435
1087369419
1087370158
1099175644
1087370131
1087370140
1099171690
1099247009
1099247777
1103425839
1137620797
1099247831
1099247823
1099247840
1099247939
1099247947
1099248340
1099248358
1103425821
1170866505
1103599501
1170866491
1137683381
1099247734
1099246983
1099247921
1099248323
1103609964
1098969639
1159881969
1139030512
1132261225
1159651432
1187077881
1128186529
1128184810
1099806237
1098969019
1098969621
1099303596
1099303898
1111427174
1099303570
1085291994
1085291986
1110015051
1109678984
1101877104
1109964201
1107022097
1107022089
1112292086
1107434981
1107434999
1101878020
1110388132
1114030640
1101877112
1094820576
1105172741
1086311352
1107020311
1107020825
1107020850
1107020299
1107020281
1103425421
1098075581
1099248005
1107338621
1140007516
1099248013
1099247998
1100680404
1103151976
1107337969
1107337942
1109052155
1108483373
1107338736
1099073523
1098948364
1099422382
1100754688
1105321704
1099175725
1100766023
1100781359
1102164993
1144199134
1105321712
1107338001
1096832075
1098970220
1086306154
1100782649
1086306189
1100803018
1086306162
1086306316
1086311344
1160055258
1100208047
1100208039
1145027191
1096832067
1096832041
1096832059
1107746150
1107746141
1113573976
1107746168
1142562801
1142572253
1144858995
1144800458
1175162956
1086974726
1108422862
1130848628
1108422871
1130320500
1110826610
1108986421
1106861711
1119599918
1137951190
1108949691
1119777395
1111605331
1110504752
1130322154
1116335553
1119106148
1097401019
1111032919
1102417140
1097850484
1097348878
1097141416
1097348908
1097144920
1100127225
1097348835
1097348185
1106075502
1098119520
1098326207
1108984801
1097144946
1108984810
1098190194
1107639931
1097173741
1108831657
1097173750
1108984631
1111650019
1108984518
1108984984
1108985026
1108984992
1132937016
1108985051
1108984976
1108984623
1148408773
1148407301
1180765014
1115741388
1108985000
1119644026
1144711374
1144711331
1119890507
1108984941
1138926997
1109052309
1108984470
1138924595
1170533140
1144711391
1144711382
1128207909
1166187661
1148779275
1108984500
1111427271
1148407319
1147061265
1144711404
1147765424
1130495890
1116387812
1137018655
1137018663
1137018647
1114622466
1146384575
1147061257
1160967851
1149825629
1149824070
1109503653
1109503645
1130848636
1130322421
1108421947
1138927004
1138924587
1116388142
1119163052
1130321913
1124239971
1108984771
1114292075
1122612658
1122768097
1086292153
1086327194
1112299099
1106860188
1099426574
1127120052
1127120036
1127120061
1149967598
1152362346
1149901074
1086630865
1138787016
1144571319
1117170086
1123308642
1116989987
1110940706
1086555367
1086555383
1148248318
1148248300
1087370174
1166978145
1170010511
1170011321
1144079988
1144142906
1140714506
1131972047
1142006037
1143153475
1153773544
1166978129
1172239281
1155582479
1174617275
1166978137
1167000866
1173141935
1109629592
1143620600
1176600477
1176600493
1141518492
1176600469
1096255485
1096255507
1098967814
1099073337
1098969540
1096250599
1115518409
1114100176
1159879247
1140859644
1154190241
1127016590
1187026543
1163022148
1179058801
1099803319
1098969302
1186107256

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
