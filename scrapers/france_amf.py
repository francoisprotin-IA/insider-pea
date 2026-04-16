"""
Scraper France - Déclarations AMF via transactions-amf.swaoo.com

V4 : Utilise les filtres de date du site pour garantir la couverture temporelle.
     Parser robuste qui gère les variations de format HTML.
"""
import re
import requests
import time
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
    """Parser robuste : cherche toutes les lignes avec au moins 8 cellules contenant une date."""
    transactions = []
    
    # Trouver toutes les <tr> dans toutes les tables
    all_rows = soup.find_all("tr")
    
    i = 0
    while i < len(all_rows):
        row = all_rows[i]
        cells = row.find_all("td")
        
        # Accepter les lignes avec 9 ou 10 cellules (le bouton + peut être absent)
        if len(cells) >= 9:
            try:
                # Vérifier que la cellule 2 ou 3 contient une date DD/MM/YYYY
                date_candidate = None
                company = ""
                date_pub = ""
                date_op = ""
                nature = ""
                instrument = ""
                isin = ""
                volume_str = ""
                price_str = ""
                amount_str = ""
                
                # Tester si c'est une ligne de résumé en cherchant le pattern date
                cell_texts = [c.get_text(strip=True) for c in cells]
                
                # Chercher 2 dates consécutives (date pub + date op) dans les premières cellules
                date_pattern = re.compile(r"^\d{2}/\d{2}/\d{4}$")
                date_positions = [j for j, t in enumerate(cell_texts) if date_pattern.match(t)]
                
                if len(date_positions) >= 2:
                    # Trouver les deux premières dates consécutives
                    dp1, dp2 = date_positions[0], date_positions[1]
                    
                    # La société est avant la première date
                    company_link = cells[0].find("a") if dp1 > 0 else None
                    company = company_link.get_text(strip=True) if company_link else cell_texts[0] if dp1 > 0 else ""
                    
                    date_pub = cell_texts[dp1]
                    date_op = cell_texts[dp2]
                    
                    # Les champs après les dates : nature, instrument, ISIN, volume, prix, montant
                    remaining = cell_texts[dp2+1:]
                    
                    if len(remaining) >= 6:
                        nature = remaining[0]
                        instrument = remaining[1]
                        isin = remaining[2]
                        volume_str = remaining[3]
                        price_str = remaining[4]
                        amount_str = remaining[5]
                    elif len(remaining) >= 4:
                        # Format condensé
                        nature = remaining[0]
                        isin = remaining[1]
                        volume_str = remaining[2]
                        amount_str = remaining[3]
                else:
                    i += 1
                    continue
                
                # Valider l'ISIN (12 chars alphanumériques)
                isin_clean = isin.strip()
                if not re.match(r"^[A-Z0-9]{10,12}$", isin_clean):
                    i += 1
                    continue
                
                # Extraire les détails de la ligne suivante
                author_full = ""
                pdf_url = ""
                decl_num = ""
                
                if i + 1 < len(all_rows):
                    detail_row = all_rows[i + 1]
                    detail_cells = detail_row.find_all("td")
                    
                    # La ligne détail a souvent 1 seule cellule avec colspan
                    if len(detail_cells) <= 3:
                        detail_text = detail_row.get_text(" ", strip=True)
                        
                        decl_match = re.search(r"Déclaration n°(\d{4}DD\d+)", detail_text)
                        if decl_match:
                            decl_num = decl_match.group(1)
                        
                        author_match = re.search(
                            r"Auteur\s*:\s*(.+?)(?:\s*Coordonnées|\s*NOM\s*:|\s*Nature\s*:|$)",
                            detail_text
                        )
                        if author_match:
                            author_full = author_match.group(1).strip()
                        
                        pdf_link = detail_row.find("a", href=re.compile(r"bdif\.amf-france\.org.*\.pdf"))
                        if pdf_link:
                            pdf_url = pdf_link.get("href", "")
                        
                        i += 1  # skip detail row
                
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
                
                if len(role) > 120:
                    role = role[:120] + "..."
                
                nature_clean = nature.strip()
                is_purchase = nature_clean in PURCHASE_NATURES
                is_sell = nature_clean in SELL_NATURES
                
                transactions.append({
                    "source": "AMF/swaoo",
                    "declaration_number": decl_num,
                    "isin": isin_clean,
                    "date": _parse_french_date(date_op),
                    "date_published": _parse_french_date(date_pub),
                    "company_name": company,
                    "insider": insider_name or "N/D",
                    "role": role,
                    "nature": nature_clean,
                    "instrument": instrument.strip(),
                    "price": _parse_french_number(price_str),
                    "quantity": _parse_french_number(volume_str),
                    "amount": _parse_french_number(amount_str),
                    "currency": "EUR",
                    "is_purchase": is_purchase,
                    "is_sell": is_sell,
                    "reference_url": pdf_url,
                })
            except Exception as e:
                pass
        
        i += 1
    
    return transactions


def scrape_all_recent(days_back: int = 180, max_pages: int = 80) -> list[dict]:
    """
    Récupère TOUTES les transactions AMF des N derniers jours.
    Utilise le filtre de dates du site pour s'assurer de couvrir toute la période.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; insider-pea-personal/1.0)",
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
    }

    cutoff_date = datetime.now() - timedelta(days=days_back)
    cutoff_str = cutoff_date.strftime("%d/%m/%Y")  # Format DD/MM/YYYY pour le site
    today_str = datetime.now().strftime("%d/%m/%Y")
    
    all_transactions = []
    empty_pages = 0
    
    for page in range(1, max_pages + 1):
        # Utiliser les filtres de date du site pour couvrir toute la période
        params = {
            "f_date_operation_min": cutoff_str,
            "f_date_operation_max": today_str,
            "f_page": page,
        }
        
        try:
            response = requests.get(BASE_URL + "/", params=params, headers=headers, timeout=30)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"  Erreur page {page}: {e}")
            time.sleep(3)
            try:
                response = requests.get(BASE_URL + "/", params=params, headers=headers, timeout=30)
                response.raise_for_status()
                print(f"  Retry page {page}: OK")
            except requests.RequestException as e2:
                print(f"  Retry échoué page {page}: {e2}")
                break
        
        soup = BeautifulSoup(response.text, "html.parser")
        page_txs = _extract_transactions_from_soup(soup)
        
        if not page_txs:
            empty_pages += 1
            print(f"  Page {page}: 0 tx parsées (page vide ou format inconnu)")
            if empty_pages >= 3:
                print(f"  3 pages vides consécutives, arrêt")
                break
            continue
        else:
            empty_pages = 0
        
        all_transactions.extend(page_txs)
        print(f"  Page {page}: {len(page_txs)} tx")
        
        # Vérifier si c'est la dernière page
        if "Page suivante" not in response.text:
            print(f"  Dernière page atteinte")
            break
        
        # Petit délai pour ne pas surcharger le serveur
        time.sleep(0.3)
    
    # Dédupliquer par numéro de déclaration
    seen = set()
    deduped = []
    for tx in all_transactions:
        key = tx.get("declaration_number") or (tx["date"] + tx["insider"] + str(tx["amount"]))
        if key not in seen:
            seen.add(key)
            deduped.append(tx)
    
    deduped.sort(key=lambda t: (t["date"], t.get("date_published", "")), reverse=True)
    
    return deduped


def scrape_france(isin: str, cutoff_date: datetime, company_name: str = None) -> list[dict]:
    """Compatibilité."""
    days = (datetime.now() - cutoff_date).days
    all_txs = scrape_all_recent(days_back=days)
    return [t for t in all_txs if t["isin"] == isin]


if __name__ == "__main__":
    print("Test: 30 derniers jours")
    txs = scrape_all_recent(days_back=30, max_pages=10)
    print(f"\nTotal: {len(txs)} tx")
    purchases = [t for t in txs if t["is_purchase"]]
    print(f"Achats: {len(purchases)}")
    top = sorted(purchases, key=lambda t: t["amount"], reverse=True)[:5]
    for t in top:
        print(f"  {t['date']} {t['company_name']} - {t['insider'][:30]} - {t['amount']:.0f}€")
