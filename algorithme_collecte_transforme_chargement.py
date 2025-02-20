###############################   Algorithme de collecte de transformation et de stockage dans S3 #########################################
###############################         Projet Big data et Cloud Computing AS3 2024/2525           ######################################
#                                  
#                                                               
###############################                                Conçu par                           ######################################
# 
#  
###############################                          Mamadou Saidou Diallo                     ####################################
###############################                              Fatimata Tall                         ################################################
###############################                      Ahmed Firhoun Oumarou Souleye                ######################################

import json
import requests
import pandas as pd
import concurrent.futures
import boto3
import time
from datetime import datetime,timedelta
from io import StringIO
import unicodedata

def lambda_handler(event, context):
    # 📌 Paramètres API NASA POWER
    parameters = "T2M,T2M_MAX,T2M_MIN,RH2M,PRECTOTCORR,WS2M,WD2M,ALLSKY_SFC_SW_DWN,PS,TS"
    date_moins_30 = datetime.today() - timedelta(days=30)
    start_date = str(date_moins_30.strftime('%Y%m%d'))
    end_date = str(date_moins_30.strftime('%Y%m%d'))

    # Paramètres S3
    S3_BUCKET = "data-meteo-as"  # Nom du bucket
    S3_KEY = f"climate_data.csv" # Nom du fichier csv
    s3_client = boto3.client("s3") # Creation du client pour interagir avec S3

    NUM_THREADS = 10      # 📌 Nombre de threads
    MAX_RETRIES = 5  # 🔄 Nombre de tentatives maximales pour une requtes
    WAIT_TIME = 20  # ⏳ Pause de 20 si surcharge API détectée

    def fetch_climate_data(region, attempt=1): # Fonction pour les donnees d'une region
        country_name, region_name, latitude, longitude = region["Country"], region["Region"], region["Latitude"], region["Longitude"]

        
        URL = f"https://power.larc.nasa.gov/api/temporal/daily/point?parameters={parameters}&community=SB&longitude={longitude}&latitude={latitude}&start={start_date}&end={end_date}&format=JSON"

        try:
            response = requests.get(URL, timeout=30) # Requetes pour recuperer les donnees depuis l'APPI

            # 🚨 Détection de surcharge API
            if response.status_code in [429, 502]:  
                print(f"⏳ Surcharge détectée ({response.status_code}) pour {region_name}, pause de {WAIT_TIME}s...")
                time.sleep(WAIT_TIME)  # 🔄 Pause de 20S
                return fetch_climate_data(region, attempt)  # 🔄 Relancer la requte apres la pause après la pause

            # Verifier si la requete a reussi
            if response.status_code != 200:
                print(f"❌ Erreur HTTP {response.status_code} pour {region_name}")
                return None

            response_data = response.json() # Convertir les donnees de la requete en un dictionnaire
            params_data = response_data.get("properties", {}).get("parameter", {}) # Recuperer les cle du dictionnaire

            # Si aucune donnee collectee retourner none
            if not params_data:
                print(f"⚠ Aucune donnée pour {region_name}, {country_name}")
                return None

            # Mettre les donnees au bon format 
            return [
                {
                    "Date": date,
                    "Country": country_name,
                    "Region": region_name,
                    "Latitude": latitude,
                    "Longitude": longitude,
                    "Temp_Avg": params_data.get("T2M", {}).get(date),
                    "Temp_Max": params_data.get("T2M_MAX", {}).get(date),
                    "Temp_Min": params_data.get("T2M_MIN", {}).get(date),
                    "Humidity": params_data.get("RH2M", {}).get(date),
                    "Precipitations": params_data.get("PRECTOTCORR", {}).get(date),
                    "Wind_Speed": params_data.get("WS2M", {}).get(date),
                    "Wind_Direction": params_data.get("WD2M", {}).get(date),
                    "Solar_Radiation": params_data.get("ALLSKY_SFC_SW_DWN", {}).get(date),
                    "Atmospheric_Pressure": params_data.get("PS", {}).get(date),
                    "Land_Surface_Temperature": params_data.get("TS", {}).get(date)
                }
                for date in params_data.get("T2M", {}).keys()
            ]

        except requests.exceptions.RequestException as e:
            print(f"❌ Erreur API pour {region_name}: {e}")
            return None

    def fetch_all_data():  # Définition de la fonction fetch_all_data pour récupérer toutes les données
        data = []  # Initialisation d'une liste vide pour stocker les données récupérées
        batch_size = 10  # Définition de la taille du lot (batch) à 10 requêtes simultanées
        total_regions = len(regions_df)  # Calcul du nombre total de régions dans le DataFrame regions_df

        for i in range(0, total_regions, batch_size):  # Boucle pour parcourir les régions par lots de batch_size
            batch = regions_df.iloc[i:i+batch_size]  # Extraction d'un lot de régions à partir de regions_df
            print(f"📡 Récupération du lot {i+1} à {min(i+batch_size, total_regions)}...")  # Affichage du lot en cours de traitement

            with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:  # Création d'un pool de threads pour exécuter les requêtes en parallèle
                futures = {executor.submit(fetch_climate_data, row): row for _, row in batch.iterrows()}  # Soumission des tâches pour chaque ligne du lot
                for future in concurrent.futures.as_completed(futures):  # Boucle pour attendre la fin de chaque tâche
                    result = future.result()  # Récupération du résultat de la tâche terminée
                    if result:  # Vérification si le résultat n'est pas vide
                        data.extend(result)  # Ajout des données récupérées à la liste data

            time.sleep(2)  # Pause de 2 secondes après chaque lot pour éviter la surcharge du serveur

        return pd.DataFrame(data)  # Retourne les données récupérées sous forme de DataFrame pandas
    

    def save_to_s3(df):  # Définition de la fonction save_to_s3 pour exporter un DataFrame sur Amazon S3
        if df.empty:  # Vérifie si le DataFrame est vide
            print("⚠ Aucun résultat à sauvegarder sur S3.")  # Affiche un message d'avertissement si le DataFrame est vide
            return  # Sort de la fonction si le DataFrame est vide

        csv_buffer = StringIO()  # Crée un buffer en mémoire pour stocker le fichier CSV
        df.to_csv(csv_buffer, index=False, sep=';', decimal='.')  # Convertit le DataFrame en CSV et l'écrit dans le buffer

        try:  # Début d'un bloc try pour gérer les erreurs potentielles
            s3_client.put_object(Bucket=S3_BUCKET, Key=S3_KEY, Body=csv_buffer.getvalue())  # Téléverse le contenu du buffer sur S3
            print(f"✅ Données sauvegardées sur S3 : s3://{S3_BUCKET}/{S3_KEY}")  # Affiche un message de succès si l'upload réussit
        except Exception as e:  # Capture toute exception qui pourrait survenir lors de l'upload
            print(f"❌ Erreur lors de l'upload sur S3: {e}")  # Affiche un message d'erreur en cas d'échec de l'upload

        
    def load_s3_data(): # Fonction pour charger un fichier a partir de S3
        try:
            response = s3_client.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
            data = response['Body'].read().decode('utf-8')
            return pd.read_csv(StringIO(data), sep=';', decimal='.')
        except Exception as e:
            print(f"❌ Erreur lors du téléchargement du fichier S3 : {e}")
            return pd.DataFrame()

    def normaliser_texte(texte):
        if isinstance(texte, str):
            # Normalisation Unicode pour corriger les caractères spéciaux
            texte = unicodedata.normalize('NFKD', texte)
            # Supprimer les caractères non ASCII si nécessaire
            texte = texte.encode('ascii', 'ignore').decode('utf-8')
        return texte


    existing_df = load_s3_data() # Charger la derniere version de la base
    regions_df = pd.read_csv("data/african_regions_with_coordinates.csv",sep=';',decimal=".") # Importer les donnees sur les coordonnees geographiques
    df = fetch_all_data() # Collecte des donnees depuis l'API de la NASA

    # Appliquer la normalisation sur toutes les colonnes de type texte
    df = df.applymap(normaliser_texte)
    
    #Correction de la date
    df['Date'] = df['Date'].astype(int)
    df['Date'] = df['Date'].astype(str)
    df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d')
    print(df['Date'].head())

     #Fusionner les deux bases
    if not existing_df.empty and not df.empty:
        # Fusionner les deux DataFrames
        merged_df = pd.concat([existing_df, df], ignore_index=True, sort=False)
        print(f"✅ Données fusionnées : {merged_df.shape[0]} lignes.")
    else:
        merged_df = df

    # Sauvegarder les données fusionnées sur S3
    save_to_s3(merged_df)

    print(f"✅ Extraction terminée avec succès ! {len(df)} lignes enregistrées sur {len(regions_df)} régions.")
    return {
        'statusCode': 200,
        'body': json.dumps(f'Hello from Lambda')
    }
