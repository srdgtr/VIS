import configparser
import os
import sys
from datetime import datetime
from ftplib import FTP
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path.cwd().parent))
from bol_export_file import get_file
from process_results.process_data import save_to_db, save_to_dropbox, save_to_dropbox_vendit

date_now = datetime.now().strftime("%c").replace(":", "-")

ini_config = configparser.ConfigParser(interpolation=None)
ini_config.read(Path.home() / "bol_export_files.ini")
scraper_name = Path.cwd().name
korting_percent = int(ini_config.get("stap 1 vaste korting", scraper_name.lower()).strip("%"))

def get_latest_file():
    with FTP() as ftp:
        ftp.connect(host=ini_config.get("visynet ftp", "server"), port=int(ini_config.get("visynet ftp", "poort")))
        ftp.login(user=ini_config.get("visynet ftp", "user"), passwd=ini_config.get("visynet ftp", "passwd"))
        # ftp.retrlines('LIST')

        names = ftp.nlst()
        assortiment = [line for line in names if "Products" in line][0]
        stock = [line for line in names if "Stock.xlsx" in line][0]

        with open(f"products_{date_now}.xlsx", "wb") as f:
            ftp.retrbinary("RETR " + assortiment, f.write)
        with open(f"stock_{date_now}.xlsx", "wb") as f:
            ftp.retrbinary("RETR " + stock, f.write)

get_latest_file()

vooraad = (
    pd.read_excel(
        max(Path.cwd().glob("products*.xlsx"), key=os.path.getctime),
    )
    .merge(pd.read_excel(max(Path.cwd().glob("stock*.xlsx"), key=os.path.getctime)), left_on= "ProductID", right_on="Product ID")
    .rename(
        columns={
            "ProductID": "sku",
            "GTIN Code": "ean",
            "Stock quantity": "stock",
            "Brand": "brand",
            "CostPriceExclVat": "price",
            "SalesPriceExclVatExclTax": "price_advice",
            "ProductDesc": "info",
            "OriginalNumber": "id",
        }
    )
    .assign(
        stock=lambda x: (np.where(pd.to_numeric(x["stock"].fillna(0)) > 6, 6, x["stock"])).astype(
            float
        ),  # niet teveel aanbieden
        eigen_sku=lambda x: scraper_name + x["sku"].astype(str),
        group = "",
        ean = lambda x: pd.to_numeric(x["ean"].fillna(x["Barcode"]), errors="coerce"),)
    .query("stock > 0")
    .query("ean == ean")
    .query("DeliveryDate != DeliveryDate") # alleen op voorraad
)


vooraad_info = vooraad[
    ["sku", "ean", "brand", "stock", "price", "price_advice", "info", "id","group"]
]

vooraad_info.to_csv(f"{scraper_name}_{date_now}.csv", index=False, encoding="utf-8-sig")

latest_file = max(Path.cwd().glob(f"{scraper_name}*.csv"), key=os.path.getctime)
save_to_dropbox(latest_file, scraper_name)

extra_columns = {'BTW code': 21,'Leverancier': scraper_name.lower()}
vendit = vooraad_info.assign(**extra_columns,ean = lambda x:x.ean.astype('string').str.split('.').str[0]).rename(
    columns={
        "eigen_sku":"Product nummer",
        "ean" :"EAN nummer",
        "price": "Inkoopprijs exclusief",
        "brand": "Merk",
        "price_advice": "Verkoopprijs inclusief",
        "hoofdgroep":"Groep Niveau 1",
        "subgroep" : "Groep Niveau 2",
        "group": "Groep Niveau 3",
        "info": "Product omschrijving",
    }
)

save_to_dropbox_vendit(vendit, scraper_name)

product_info = vooraad_info.rename(
    columns={
        # "sku":"onze_sku",
        # "ean":"ean",
        "brand": "merk",
        "stock": "voorraad",
        "price": "inkoop_prijs",
        # :"promo_inkoop_prijs",
        # :"promo_inkoop_actief",
        # "group" :"category",
        "price_advice": "advies_prijs",
        "info": "omschrijving",
    }
).assign(onze_sku=lambda x: scraper_name + x["sku"].astype(str), import_date=datetime.now())

save_to_db(product_info)