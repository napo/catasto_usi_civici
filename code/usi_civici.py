#!/usr/bin/env python
# coding: utf-8

# In[19]:


import geopandas as gpd
import pandas as pd
import os 
import glob
import requests
import zipfile
dest_doc = ".." + os.sep + "docs"
donwload_url = "https://catastotn.tndigit.it/export_semestrale_VL_PUBB/IDR0020230701_TIPOCATSH_CCXXX.zip"
url_csv = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSPeLuWTTF1JhWOhhR_ZJmSLBJhMqcJ771xWUeNnuX2co7aV2k2UytMRWU3AZdgfP4gIsWZZHsmx3T7/pub?output=csv"


# In[4]:


df = pd.read_csv(url_csv)
codici_catastali = df.codice_comune_catastale.unique()


# In[5]:


gdflist = []
for codice in codici_catastali:
    codice = str(codice).zfill(3)
    url = donwload_url.replace("XXX",codice)
    response = requests.get(url)
    # Elenca i nomi dei file che vuoi estrarre
    files_to_extract = []
    suffix = "_vl_uniqueparcel_poly"
    suffix = "_vl_parcel_poly"
    files_to_extract.append(codice + suffix + ".prj")
    files_to_extract.append(codice + suffix + ".shp")
    files_to_extract.append(codice + suffix + ".shx")
    files_to_extract.append(codice + suffix + ".dbf")
    if response.status_code == 200:
        with open("file.zip", "wb") as file:
            file.write(response.content)
        with zipfile.ZipFile("file.zip", "r") as zip_ref:
            for file_name in zip_ref.namelist():
                if file_name in files_to_extract:
                    zip_ref.extract(file_name)
        gdf = gpd.read_file(codice + suffix + ".shp")
        crs = gdf.crs
        gdflist.append(gdf)
        os.remove("file.zip")
        for shp in files_to_extract:
            os.remove(shp)
parcels = gpd.GeoDataFrame(pd.concat(gdflist, ignore_index=True), crs=crs)


# In[6]:


parcels['catasto'] = ""
parcels['comune'] = "NO"
parcels['prg1'] = ""
parcels['prg2'] = ""
parcels['prg3'] = ""
parcels['prg4'] = ""
parcels['prg5'] = ""
parcels['aggiornamento'] = ""
parcels["ettari"] = ""
notfound = []
for idx, row in df.iterrows():
    codice_comune_catastale= row['codice_comune_catastale']
    codice_particella = row['codice_particella']  
    p = parcels[(parcels.PT_CODE == codice_particella) & (parcels.PT_CCAT == codice_comune_catastale)]  
    if p.shape[0] >0:
        parcels.at[p.index[0],"comune"] = row['comune_ammistrativo']
        parcels.at[p.index[0],"catasto"] = row['nome_comune_catastale']
        parcels.at[p.index[0],"prg1"] = row['destinazione_uso_1']
        parcels.at[p.index[0],"prg2"] = row['destinazione_uso_1']
        parcels.at[p.index[0],"prg3"] = row['commento']
        parcels.at[p.index[0],"prg4"] = row['destinazione_uso_1']
        parcels.at[p.index[0],"prg5"] = row['destinazione_uso_1']
        parcels.at[p.index[0],"prg6"] = row['destinazione_uso_1']
        parcels.at[p.index[0],"aggiornamento"] = row['data_ultimo_aggiornamento_dati']
        parcels.at[p.index[0],"ettari"] = round(p.geometry.area[p.index[0]]/1000,2)
    else:
        nf = {}
        nf['codice_particella'] = codice_particella
        nf['codice_comune_catastale'] = codice_comune_catastale
        notfound.append(nf)
        


# In[7]:


parcels.fillna("non disponibile", inplace=True)


# In[8]:


usi_civici = parcels[parcels.comune != "NO"]
usi_civici=usi_civici.to_crs(epsg=4326)
usi_civici.to_file(dest_doc + os.sep + "usi_civici.geojson")


# In[10]:


usi_civici_edifici = usi_civici[usi_civici['PT_CODE'].str.startswith('.')]
usi_civici_terreni = usi_civici[~usi_civici['PT_CODE'].str.startswith('.')]


# In[22]:


pd.DataFrame(notfound).to_excel(dest_doc + os.sep + "particelle_non_trovate.xlsx")
usi_civici_terreni.to_file(dest_doc + os.sep +"usi_civici_edifici.geojson")
usi_civici_edifici.to_file(dest_doc + os.sep +"usi_civici_terreni.geojson")
