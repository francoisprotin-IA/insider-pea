"""
Orchestrateur principal - Insider PEA V3
Récupère TOUTES les transactions AMF récentes (approche InsiderScreener),
puis enrichit avec Yahoo Finance pour les tickers connus.
"""
import json
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from scrapers.france_amf import scrape_all_recent
from scrapers.yahoo_finance import enrich_with_yahoo
from scrapers.scoring import compute_insider_score, compute_tech_guard, compute_verdict

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)

# Mapping ISIN -> ticker Yahoo Finance (pour enrichissement optionnel)
# Plus on en ajoute, plus on aura d'infos analystes dans le dashboard.
# Pour les ISINs absents, seule la partie "achat d'initié" sera affichée (pas de potentiel analystes).
ISIN_TO_TICKER = {
    # CAC 40
    "FR0000120271": "TTE.PA", "FR0000131104": "BNP.PA", "FR0000120578": "SAN.PA",
    "FR0000121014": "MC.PA", "FR0000121972": "SU.PA", "FR0000120073": "AI.PA",
    "FR0000125338": "CAP.PA", "FR0000051807": "TEP.PA", "FR0000120693": "RI.PA",
    "FR0014003TT8": "DSY.PA", "FR0000120321": "OR.PA", "FR0000052292": "RMS.PA",
    "FR0000121485": "KER.PA", "FR0000045072": "ACA.PA", "FR0000120628": "CS.PA",
    "NL0000235190": "AIR.PA", "FR0000073272": "SAF.PA", "FR001400AJ45": "ML.PA",
    "FR0000125486": "DG.PA", "FR0000133308": "ORA.PA", "FR0010220475": "PUB.PA",
    "FR0000121667": "EL.PA", "FR0000120172": "CA.PA", "FR0000130577": "BN.PA",
    # SBF 120 / Mid caps où les insiders achètent souvent
    "FR0013230612": "TKO.PA",       # Tikehau Capital
    "FR0013269123": "RUI.PA",       # Rubis
    "FR0000054470": "UBI.PA",       # Ubisoft
    "FR0010588079": "FREY.PA",      # Frey
    "FR0010626500": "ARG.PA",       # Argan
    "FR0000031122": "EIFF.PA",      # Société Tour Eiffel
    "FR0000120164": "LNA.PA",       # LNA Santé
    "FR0000031775": "VCT.PA",       # Vicat
    "FR0000053381": "CGM.PA",       # Cegedim
    "FR0000066755": "PIG.PA",       # Haulotte
    "FR0013344173": "RBO.PA",       # Roche Bobois
    "FR0000062739": "ABCA.PA",      # ABC Arbitrage
    "FR0000121709": "SK.PA",        # SEB (Groupe SEB)
    "FR0014000MR3": "EXENS.PA",     # Exosens
    "FR0004040608": "ABCA.PA",      # ABC Arbitrage (ISIN alternatif)
    "FR0000074148": "FNAC.PA",      # Fnac Darty
    "FR0000125007": "SGO.PA",       # Saint-Gobain
    "FR0000130809": "SU.PA",        # Société Générale
    "FR0000120404": "ENGI.PA",      # Accor (ex Engie)
    "FR0010208488": "ENGI.PA",      # Engie
    "FR0000053225": "DAST.PA",      # Dassault Aviation
    "FR0000184798": "GDS.PA",       # Gecina
    "FR0010040865": "GFC.PA",       # Coface
    "FR0000035370": "GTT.PA",       # GTT
    # International
    "BE0974293251": "ABI.BR",       # AB InBev
    "NL0011821202": "INGA.AS",      # ING
    "NL0010273215": "ASML.AS",      # ASML
    "IT0005678104": "ALKLN.PA",     # Kaleon (Euronext Growth Paris)
}


def main():
    print("=" * 60)
    print(f"INSIDER PEA V3 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # On récupère 180 jours, le dashboard filtre ensuite par période
    days_back = 180

    # 1. Scraping global (toutes entreprises)
    print(f"\n[1/3] Scraping AMF - {days_back} derniers jours (toutes entreprises)...")
    all_transactions = scrape_all_recent(days_back=days_back, max_pages=80)
    print(f"\nTotal : {len(all_transactions)} transactions récupérées")

    purchases = [t for t in all_transactions if t["is_purchase"]]
    sells = [t for t in all_transactions if t["is_sell"]]
    print(f"  Achats : {len(purchases)}")
    print(f"  Ventes : {len(sells)}")

    # Extraire toutes les entreprises uniques
    unique_companies = {}
    for tx in all_transactions:
        isin = tx["isin"]
        if isin not in unique_companies:
            unique_companies[isin] = {
                "isin": isin,
                "name": tx["company_name"],
                "ticker": ISIN_TO_TICKER.get(isin),
            }

    print(f"  Entreprises uniques : {len(unique_companies)}")

    # 2. Enrichissement Yahoo Finance (seulement pour les tickers connus)
    print(f"\n[2/3] Enrichissement Yahoo Finance...")
    companies_with_tickers = [c for c in unique_companies.values() if c["ticker"]]
    print(f"  {len(companies_with_tickers)} entreprises avec ticker connu (sur {len(unique_companies)})")

    company_data = dict(unique_companies)
    for i, co in enumerate(companies_with_tickers, 1):
        try:
            quote = enrich_with_yahoo(co["ticker"])
            if quote:
                company_data[co["isin"]]["quote"] = quote
                price = quote.get("currentPrice", "N/A")
                print(f"  [{i}/{len(companies_with_tickers)}] {co['name']}: {price}")
            else:
                company_data[co["isin"]]["quote"] = None
        except Exception as e:
            company_data[co["isin"]]["quote"] = None
            print(f"  [{i}/{len(companies_with_tickers)}] {co['name']}: erreur {str(e)[:60]}")
        time.sleep(0.1)

    # S'assurer que toutes les entreprises ont un champ quote
    for isin in company_data:
        if "quote" not in company_data[isin]:
            company_data[isin]["quote"] = None

    # 3. Calcul des scores (uniquement sur les ACHATS)
    print(f"\n[3/3] Calcul des scores...")

    purchases_by_isin = {}
    for tx in purchases:
        if (tx.get("amount") or 0) < 1000:  # Filtrer le bruit (< 1k€)
            continue
        isin = tx["isin"]
        if isin not in purchases_by_isin:
            purchases_by_isin[isin] = []
        purchases_by_isin[isin].append(tx)

    recommendations = []
    for isin, txs in purchases_by_isin.items():
        co = company_data.get(isin, {"isin": isin, "name": txs[0]["company_name"], "ticker": None, "quote": None})

        ins_score = compute_insider_score(txs)
        tech = compute_tech_guard(co.get("quote"))
        total = max(ins_score, ins_score + tech["adj"])
        verdict = compute_verdict(total)

        unique_insiders = len(set(tx["insider"] for tx in txs))
        total_amount = sum(tx.get("amount", 0) or 0 for tx in txs)
        last_buy = max(tx["date"] for tx in txs)
        top_tx = max(txs, key=lambda t: t.get("amount", 0) or 0)

        recommendations.append({
            "isin": isin,
            "name": co["name"],
            "ticker": co.get("ticker"),
            "insider_score": ins_score,
            "tech_adj": tech["adj"],
            "tech_detail": tech,
            "total_score": total,
            "verdict": verdict,
            "tx_count": len(txs),
            "total_amount": total_amount,
            "unique_insiders": unique_insiders,
            "last_buy": last_buy,
            "top_insider": top_tx["insider"],
            "top_role": top_tx.get("role", ""),
            "quote": co.get("quote"),
            "transactions": sorted(txs, key=lambda t: t["date"], reverse=True),
        })

    recommendations.sort(key=lambda r: r["total_score"], reverse=True)

    output = {
        "generated_at": datetime.now().isoformat(),
        "transactions_count": len(purchases),
        "sells_count": len(sells),
        "total_transactions": len(all_transactions),
        "companies_with_buys": len(purchases_by_isin),
        "days_covered": days_back,
        "recommendations": recommendations,
        "transactions": all_transactions,
        "company_data": company_data,
    }

    output_path = DATA_DIR / "latest.json"
    output_path.write_text(json.dumps(output, ensure_ascii=False, indent=2, default=str))
    print(f"\n✅ Données écrites dans {output_path}")
    print(f"   Transactions totales : {len(all_transactions)}")
    print(f"   Achats : {len(purchases)}")
    print(f"   Ventes : {len(sells)}")
    print(f"   Entreprises avec achats : {len(purchases_by_isin)}")

    strong = [r for r in recommendations if r["total_score"] >= 85]
    buys = [r for r in recommendations if 65 <= r["total_score"] < 85]
    interesting = [r for r in recommendations if 45 <= r["total_score"] < 65]
    print(f"   🟢 Achat Fort : {len(strong)}")
    print(f"   🟡 Achat : {len(buys)}")
    print(f"   🔵 Intéressant : {len(interesting)}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
