"""
Scraper France - Déclarations AMF via transactions-amf.swaoo.com

V3 : Récupère TOUTES les transactions récentes (pas par entreprise).
C'est l'approche InsiderScreener : on montre ce qui se passe, peu importe l'émetteur.
Le dashboard filtre ensuite par période (7j, 30j, 90j, 180j).
"""
import re
import requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

BASE_URL = "https://transactions-amf.swaoo.com"

PURCHASE_NATURES = {"Acquisition", "Souscription"}
SELL_NATURES = {"Cession"}


def _parse_french_number(s):
    if not s:
        return 0.0
    s = str(s).strip()
    s = s.replace("\u202f", "").replace("\xa0", "").replace(" ", "")
    s = s.replace("€", "").replace("\u20ac", "").strip()
    if "," in s and "." not in s:
        s = s.replace(",", ".")
    elif "," in s and "." in s:
        s = s.replace(",", "")
    try:
        return float(s)
    except ValueError:
        return 0.0


def _parse_french_date(s):
    if not s:
        return ""
    s = str(s).strip()
    parts = s.split("/")
    if len(parts) == 3:
        dd, mm, yyyy = parts
        return f"{yyyy}-{mm.zfill(2)}-{dd.zfill(2)}"
    return s


def _extract_transactions_from_soup(soup):
    transactions = []
    tables = soup.find_all("table")
    
    for table in tables:
        rows = table.find_all("tr")
        i = 0
        while i < len(rows):
            row = rows[i]
            cells = row.find_all("td")
            
            if len(cells) == 10:
                try:
                    company_link = cells[0].find("a")
                    company = company_link.get_text(strip=True) if company_link else cells[0].get_text(strip=True)
                    
                    date_pub = cells[1].get_text(strip=True)
                    date_op = cells[2].get_text(strip=True)
                    nature = cells[3].get_text(strip=True)
                    instrument = cells[4].get_text(strip=True)
                    isin = cells[5].get_text(strip=True)
                    volume = cells[6].get_text(strip=True)
                    price = cells[7].get_text(strip=True)
                    amount = cells[8].get_text(strip=True)
                    
                    if not re.match(r"\d{2}/\d{2}/\d{4}", date_op):
                        i += 1
                        continue
                    if not re.match(r"^[A-Z0-9]{12}$", isin):
                        i += 1
                        continue
                    
                    author_full = ""
                    pdf_url = ""
                    decl_num = ""
                    
                    if i + 1 < len(rows):
                        detail_row = rows[i + 1]
                        detail_text = detail_row.get_text(" ", strip=True)
                        
                        decl_match = re.search(r"Déclaration n°(\d{4}DD\d+)", detail_text)
                        if decl_match:
                            decl_num = decl_match.group(1)
                        
                        author_match = re.search(
                            r"Auteur\s*:\s*([^*]+?)(?:\s*Coordonnées|\s*NOM\s*:|\s*Nature\s*:|$)",
                            detail_text
                        )
                        if author_match:
                            author_full = author_match.group(1).strip()
                        
                        pdf_link = detail_row.find("a", href=re.compile(r"bdif\.amf-france\.org.*\.pdf"))
                        if pdf_link:
                            pdf_url = pdf_link.get("href", "")
                    
                    insider_name = author_full
                    role = "N/D"
                    if "personne morale liée à" in author_full:
                        match = re.search(r"personne morale liée à\s+([^,]+),\s*(.+)", author_full)
                        if match:
                            insider_name = match.group(1).strip()
                            role = match.group(2).strip()
                    elif "," in author_full:
                        parts = author_full.split(",", 1)
                        insider_name = parts[0].strip()
                        role = parts[1].strip() if len(parts) > 1 else "N/D"
                    
                    if len(role) > 100:
                        role = role[:100] + "..."
                    
                    nature_clean = nature.strip()
                    is_purchase = nature_clean in PURCHASE_NATURES
                    is_sell = nature_clean in SELL_NATURES
                    
                    transactions.append({
                        "source": "AMF/swaoo",
                        "declaration_number": decl_num,
                        "isin": isin,
                        "date": _parse_french_date(date_op),
                        "date_published": _parse_french_date(date_pub),
                        "company_name": company,
                        "insider": insider_name,
                        "role": role,
                        "nature": nature_clean,
                        "instrument": instrument,
                        "price": _parse_french_number(price),
                        "quantity": _parse_french_number(volume),
                        "amount": _parse_french_number(amount),
                        "currency": "EUR",
                        "is_purchase": is_purchase,
                        "is_sell": is_sell,
                        "reference_url": pdf_url,
                    })
                    
                    i += 2
                    continue
                except Exception:
                    pass
            i += 1
    
    return transactions


def scrape_all_recent(days_back: int = 180, max_pages: int = 80) -> list[dict]:
    """
    Récupère TOUTES les transactions AMF des N derniers jours.
    
    Args:
        days_back: Nombre de jours à récupérer (défaut 180 = 6 mois)
        max_pages: Nombre max de pages à parcourir (15 tx/page, 80 pages = ~1200 tx)
    
    Returns:
        Liste de transactions triées par date décroissante
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; insider-pea-personal/1.0)",
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
    }

    cutoff_date = datetime.now() - timedelta(days=days_back)
    cutoff_str = cutoff_date.strftime("%Y-%m-%d")
    
    all_transactions = []
    consecutive_old_pages = 0
    
    for page in range(1, max_pages + 1):
        url = f"{BASE_URL}/?f_page={page}"
        
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"  Erreur page {page}: {e}")
            # Retry once after 3 seconds
            import time
            time.sleep(3)
            try:
                response = requests.get(url, headers=headers, timeout=30)
                response.raise_for_status()
                print(f"  Retry page {page}: OK")
            except requests.RequestException as e2:
                print(f"  Retry échoué page {page}: {e2}")
                break
        
        soup = BeautifulSoup(response.text, "html.parser")
        page_txs = _extract_transactions_from_soup(soup)
        
        if not page_txs:
            print(f"  Page {page}: aucune transaction trouvée, arrêt")
            break
        
        in_window = [t for t in page_txs if t["date"] >= cutoff_str]
        out_of_window = [t for t in page_txs if t["date"] < cutoff_str]
        
        all_transactions.extend(in_window)
        print(f"  Page {page}: {len(page_txs)} tx ({len(in_window)} dans la fenêtre {days_back}j)")
        
        # Si la page entière est hors fenêtre, on compte les pages consécutives
        if len(in_window) == 0:
            consecutive_old_pages += 1
            if consecutive_old_pages >= 5:
                print(f"  5 pages consécutives hors fenêtre, arrêt")
                break
        else:
            consecutive_old_pages = 0
        
        if "Page suivante" not in response.text:
            break
    
    # Dédupliquer
    seen = set()
    deduped = []
    for tx in all_transactions:
        key = tx.get("declaration_number") or (tx["date"] + tx["insider"] + str(tx["amount"]))
        if key not in seen:
            seen.add(key)
            deduped.append(tx)
    
    # Trier par date de transaction décroissante
    deduped.sort(key=lambda t: (t["date"], t.get("date_published", "")), reverse=True)
    
    return deduped


def scrape_france(isin: str, cutoff_date: datetime, company_name: str = None) -> list[dict]:
    """Compatibilité - filtre par ISIN."""
    days = (datetime.now() - cutoff_date).days
    all_txs = scrape_all_recent(days_back=days)
    return [t for t in all_txs if t["isin"] == isin]


if __name__ == "__main__":
    print("Test: récupération des transactions des 30 derniers jours")
    txs = scrape_all_recent(days_back=30, max_pages=10)
    print(f"\nTotal: {len(txs)} transactions")
    
    purchases = [t for t in txs if t["is_purchase"]]
    print(f"Achats: {len(purchases)}")
    
    top_buys = sorted(purchases, key=lambda t: t["amount"], reverse=True)[:5]
    print(f"\nTop 5:")
    for t in top_buys:
        print(f"  {t['date']} {t['company_name']} - {t['insider']} - {t['amount']:.0f}€")
