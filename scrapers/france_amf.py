"""
Scraper France - Déclarations AMF via transactions-amf.swaoo.com
Source: agrégateur indépendant qui traite quotidiennement les PDF AMF
(lestransactions.fr est offline depuis fin 2024)

Parse le HTML de la page de résultats de recherche.
"""
import re
import requests
from datetime import datetime

BASE_URL = "https://transactions-amf.swaoo.com"

# Natures reconnues comme ACHATS (signaux positifs pour le scoring insider)
PURCHASE_NATURES = {"Acquisition", "Souscription"}


def _parse_french_number(s: str) -> float:
    """Parse un nombre format français '1 923,4500' → 1923.45"""
    if not s:
        return 0.0
    # Enlever les espaces (séparateur de milliers français, incluant espaces insécables)
    s = s.replace("\u202f", "").replace("\xa0", "").replace(" ", "")
    # Enlever le symbole € et autres
    s = s.replace("€", "").replace("\u20ac", "").strip()
    # Remplacer virgule décimale par point
    s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return 0.0


def _parse_french_date(s: str) -> str:
    """Parse '14/04/2026' → '2026-04-14'"""
    if not s:
        return ""
    s = s.strip()
    parts = s.split("/")
    if len(parts) == 3:
        dd, mm, yyyy = parts
        return f"{yyyy}-{mm.zfill(2)}-{dd.zfill(2)}"
    return s


def _extract_transactions_from_html(html: str) -> list[dict]:
    """
    Extrait les transactions du HTML de transactions-amf.swaoo.com.
    
    Structure de chaque transaction :
    - Une <tr> avec 10 <td> pour le résumé
    - Une <tr> qui suit avec les détails (auteur, LEI, etc.)
    """
    transactions = []

    # Pattern pour les lignes de résumé (10 cellules)
    # Le lien de la société est dans <a>, les autres cellules sont en texte simple
    summary_pattern = re.compile(
        r'<tr>\s*'
        r'<td[^>]*>\s*<a[^>]*>\s*([^<]+?)\s*</a>\s*</td>\s*'   # 1. Société
        r'<td[^>]*>\s*(\d{2}/\d{2}/\d{4})\s*</td>\s*'           # 2. Date publication
        r'<td[^>]*>\s*(\d{2}/\d{2}/\d{4})\s*</td>\s*'           # 3. Date opération
        r'<td[^>]*>\s*([^<]+?)\s*</td>\s*'                       # 4. Nature
        r'<td[^>]*>\s*([^<]+?)\s*</td>\s*'                       # 5. Instrument
        r'<td[^>]*>\s*([A-Z0-9]+)\s*</td>\s*'                    # 6. ISIN
        r'<td[^>]*>\s*([\d\s,\.\u202f\xa0]+?)\s*</td>\s*'       # 7. Volume
        r'<td[^>]*>\s*([\d\s,\.\u202f\xa0]+?)\s*</td>\s*'       # 8. Prix
        r'<td[^>]*>\s*([\d\s,\.\u202f\xa0€\u20ac]+?)\s*</td>',  # 9. Montant
        re.DOTALL
    )

    matches = summary_pattern.findall(html)

    # Extraire les numéros de déclaration dans l'ordre d'apparition
    declaration_pattern = re.compile(r'Déclaration n°(\d{4}DD\d+)')
    declaration_nums = declaration_pattern.findall(html)

    # Extraire les auteurs (après "<strong>Auteur :</strong>")
    # Format : <strong>Auteur :</strong> Prénom Nom, Fonction
    author_pattern = re.compile(
        r'<strong>\s*Auteur\s*:\s*</strong>\s*([^<\n]+?)\s*(?:</li>|<br|<li)',
        re.DOTALL
    )
    authors = author_pattern.findall(html)

    # Extraire les liens PDF des déclarations
    pdf_pattern = re.compile(r'(https://bdif\.amf-france\.org/back/api/v1/documents/\d+/\d+DD\d+/[A-F0-9]+\.pdf)')
    pdf_urls = pdf_pattern.findall(html)

    # Construire les transactions
    for i, m in enumerate(matches):
        company, date_pub, date_op, nature, instrument, isin, volume, price, amount = m

        author_full = authors[i].strip() if i < len(authors) else ""
        pdf_url = pdf_urls[i] if i < len(pdf_urls) else ""
        decl_num = declaration_nums[i] if i < len(declaration_nums) else ""

        # Parser l'auteur : "Nom Prénom, Fonction"
        if "," in author_full:
            parts = author_full.split(",", 1)
            insider_name = parts[0].strip()
            role = parts[1].strip()
        else:
            insider_name = author_full.strip() or "N/D"
            role = "N/D"

        nature_clean = nature.strip()
        is_purchase = nature_clean in PURCHASE_NATURES

        transactions.append({
            "source": "AMF/swaoo",
            "declaration_number": decl_num,
            "isin": isin.strip(),
            "date": _parse_french_date(date_op),
            "date_published": _parse_french_date(date_pub),
            "company_name": company.strip(),
            "insider": insider_name,
            "role": role,
            "nature": nature_clean,
            "instrument": instrument.strip(),
            "price": _parse_french_number(price),
            "quantity": _parse_french_number(volume),
            "amount": _parse_french_number(amount),
            "currency": "EUR",
            "is_purchase": is_purchase,
            "reference_url": pdf_url,
        })

    return transactions


def scrape_france(isin: str, cutoff_date: datetime) -> list[dict]:
    """
    Récupère les transactions pour un ISIN via transactions-amf.swaoo.com.
    
    Args:
        isin: Code ISIN (ex: FR0000120271)
        cutoff_date: Ne garder que les transactions après cette date
    
    Returns:
        Liste de transactions normalisées (triées par date décroissante)
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; insider-pea-personal/1.0; research use)",
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
    }

    all_transactions = []
    page = 1
    max_pages = 5  # Garde-fou : 5 pages × ~15 tx/page = 75 tx max par ISIN

    while page <= max_pages:
        params = {
            "f_keyword": isin,
            "f_page": page,
        }
        try:
            response = requests.get(
                BASE_URL + "/",
                params=params,
                headers=headers,
                timeout=30,
            )
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"    Erreur réseau page {page}: {e}")
            break

        page_txs = _extract_transactions_from_html(response.text)

        if not page_txs:
            break

        # Filtrer par ISIN exact (la recherche peut parfois retourner des sociétés liées)
        page_txs = [t for t in page_txs if t["isin"] == isin]

        # Filtrer par date cutoff
        cutoff_str = cutoff_date.strftime("%Y-%m-%d")
        before_cutoff = [t for t in page_txs if t["date"] < cutoff_str]
        after_cutoff = [t for t in page_txs if t["date"] >= cutoff_str]

        all_transactions.extend(after_cutoff)

        # Si on a atteint des transactions plus anciennes que le cutoff, on s'arrête
        if before_cutoff:
            break

        # Détection de la pagination
        if "Page suivante" not in response.text and "f_page=" + str(page + 1) not in response.text:
            break

        page += 1

    # Dédupliquer par numéro de déclaration (au cas où)
    seen = set()
    deduped = []
    for tx in all_transactions:
        key = tx.get("declaration_number") or (tx["date"] + tx["insider"] + str(tx["amount"]))
        if key not in seen:
            seen.add(key)
            deduped.append(tx)

    # Trier par date de transaction décroissante
    deduped.sort(key=lambda t: t["date"], reverse=True)

    return deduped


if __name__ == "__main__":
    from datetime import timedelta
    cutoff = datetime.now() - timedelta(days=180)

    # Test avec TotalEnergies
    print("Test 1: TotalEnergies (FR0000120271)")
    txs = scrape_france("FR0000120271", cutoff)
    print(f"  Trouvé {len(txs)} transactions")
    for tx in txs[:3]:
        print(f"  {tx['date']} - {tx['insider']} ({tx['role']}) - {tx['nature']} {tx['amount']:.0f}€")

    print("\nTest 2: Rubis (FR0013269123)")
    txs = scrape_france("FR0013269123", cutoff)
    print(f"  Trouvé {len(txs)} transactions")
    for tx in txs[:3]:
        print(f"  {tx['date']} - {tx['insider']} ({tx['role']}) - {tx['nature']} {tx['amount']:.0f}€")
