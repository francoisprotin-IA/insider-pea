"""
Scraper France - Déclarations AMF via transactions-amf.swaoo.com
Source: agrégateur indépendant qui traite quotidiennement les PDF AMF
(lestransactions.fr est offline depuis fin 2024)

V2: Parsing HTML robuste avec BeautifulSoup
    Recherche par nom de société via /societes/NOM/ (plus fiable que par ISIN)
"""
import re
import requests
from datetime import datetime
from bs4 import BeautifulSoup

BASE_URL = "https://transactions-amf.swaoo.com"

# Natures reconnues comme ACHATS (signaux positifs pour le scoring insider)
PURCHASE_NATURES = {"Acquisition", "Souscription"}


def _parse_french_number(s):
    """Parse un nombre format français '1 923,4500' → 1923.45"""
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
    """Parse '14/04/2026' → '2026-04-14'"""
    if not s:
        return ""
    s = str(s).strip()
    parts = s.split("/")
    if len(parts) == 3:
        dd, mm, yyyy = parts
        return f"{yyyy}-{mm.zfill(2)}-{dd.zfill(2)}"
    return s


def _extract_transactions_from_soup(soup):
    """Extrait les transactions depuis le BeautifulSoup parsé."""
    transactions = []
    tables = soup.find_all("table")
    
    for table in tables:
        rows = table.find_all("tr")
        i = 0
        while i < len(rows):
            row = rows[i]
            cells = row.find_all("td")
            
            # Une ligne de résumé a 10 cellules
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
                    
                    # Validation basique
                    if not re.match(r"\d{2}/\d{2}/\d{4}", date_op):
                        i += 1
                        continue
                    if not re.match(r"^[A-Z0-9]{12}$", isin):
                        i += 1
                        continue
                    
                    # Détails dans la ligne suivante
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
                    
                    # Parser l'auteur
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
                        "reference_url": pdf_url,
                    })
                    
                    i += 2
                    continue
                except Exception:
                    pass
            i += 1
    
    return transactions


def _get_company_slug(company_name):
    """Ex: 'BNP Paribas' → 'BNP+PARIBAS'"""
    return company_name.upper().strip().replace(" ", "+")


def scrape_france(isin: str, cutoff_date: datetime, company_name: str = None) -> list[dict]:
    """
    Récupère les transactions pour une société via transactions-amf.swaoo.com.
    
    Args:
        isin: Code ISIN (ex: FR0000120271)
        cutoff_date: Ne garder que les transactions après cette date
        company_name: Nom de la société (recommandé) pour utiliser /societes/NOM/
    
    Returns:
        Liste de transactions normalisées (triées par date décroissante)
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; insider-pea-personal/1.0)",
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
    }

    all_transactions = []
    
    # Essayer les différentes stratégies de recherche
    urls_to_try = []
    if company_name:
        slug = _get_company_slug(company_name)
        urls_to_try.append(f"{BASE_URL}/societes/{slug}/")
    urls_to_try.append(f"{BASE_URL}/?f_keyword={isin}")
    
    for url in urls_to_try:
        page = 1
        max_pages = 5
        
        while page <= max_pages:
            if "?" in url:
                page_url = f"{url}&f_page={page}"
            else:
                page_url = f"{url}?f_page={page}"
            
            try:
                response = requests.get(page_url, headers=headers, timeout=30)
                response.raise_for_status()
            except requests.RequestException as e:
                print(f"    Erreur réseau {page_url}: {e}")
                break
            
            soup = BeautifulSoup(response.text, "html.parser")
            page_txs = _extract_transactions_from_soup(soup)
            
            if not page_txs:
                break
            
            # Filtrer par ISIN exact
            page_txs = [t for t in page_txs if t["isin"] == isin]
            
            # Filtrer par date cutoff
            cutoff_str = cutoff_date.strftime("%Y-%m-%d")
            before_cutoff = [t for t in page_txs if t["date"] < cutoff_str]
            after_cutoff = [t for t in page_txs if t["date"] >= cutoff_str]
            
            all_transactions.extend(after_cutoff)
            
            if before_cutoff:
                break
            
            if "Page suivante" not in response.text and f"f_page={page + 1}" not in response.text:
                break
            
            page += 1
        
        if all_transactions:
            break
    
    # Dédupliquer par numéro de déclaration
    seen = set()
    deduped = []
    for tx in all_transactions:
        key = tx.get("declaration_number") or (tx["date"] + tx["insider"] + str(tx["amount"]))
        if key not in seen:
            seen.add(key)
            deduped.append(tx)
    
    deduped.sort(key=lambda t: t["date"], reverse=True)
    return deduped


if __name__ == "__main__":
    from datetime import timedelta
    cutoff = datetime.now() - timedelta(days=180)
    
    print("Test: TotalEnergies (FR0000120271)")
    txs = scrape_france("FR0000120271", cutoff, company_name="TotalEnergies")
    print(f"  Trouvé {len(txs)} transactions")
    for tx in txs[:3]:
        print(f"  {tx['date']} - {tx['insider']} - {tx['nature']} {tx['amount']:.0f}€")
