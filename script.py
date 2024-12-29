import json
from datetime import datetime, timedelta
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from google.oauth2 import service_account
from collections import defaultdict
from pymongo import MongoClient

# Configuration MongoDB
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = ""
COLLECTION_NAME = "" 

def sample_run_report(property_id, source_name):
    SERVICE_ACCOUNT_FILE = ".json"
    # Charger les credentials directement à partir du fichier de compte de service
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE
    )

    # Initialiser le client avec les credentials
    client = BetaAnalyticsDataClient(credentials=credentials)

    request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name="date")],  # Ajouter la dimension "date"
        metrics=[Metric(name="activeUsers")],
        date_ranges=[DateRange(start_date="2024-01-01", end_date="today")],  # Définir la période de temps depuis janvier
    )
    response = client.run_report(request)
    
    # Pré-remplir le dictionnaire des résultats mensuels avec tous les mois de l'année
    monthly_results = defaultdict(int, {datetime(2024, month, 1).strftime('%b'): 0 for month in range(1, 13)})
    for row in response.rows:
        date_str = row.dimension_values[0].value
        date_obj = datetime.strptime(date_str, '%Y%m%d')
        month_str = date_obj.strftime('%b')  # Utiliser les abréviations des noms de mois (par exemple, "Oct" pour octobre)
        monthly_results[month_str] += int(row.metric_values[0].value)


    date_30_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    request2 = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name="date")],  # Ajouter la dimension "date"
        metrics=[Metric(name="activeUsers")],
        date_ranges=[DateRange(start_date=date_30_days_ago, end_date="today")],  # 30 derniers jours
    )
    response2 = client.run_report(request2)
    
    nb_user_30_days = sum(
        int(row.metric_values[0].value) for row in response2.rows
    )
    # Créer le document à insérer dans MongoDB
    document = {
        "propertyId": property_id,
        "source": source_name,
        "data": [{"month": month, "activeUsers": active_users} for month, active_users in sorted(monthly_results.items(), key=lambda x: datetime.strptime(x[0], '%b'))],
        "nbUser30Day":nb_user_30_days
    }

    # Insérer les données dans MongoDB
    insert_into_mongodb(document)

def insert_into_mongodb(document):
    # Connectez-vous à MongoDB
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    # Obtenez la collection
    collection = db[COLLECTION_NAME]

    # Remplacer le document existant (basé sur `propertyId`) ou insérer un nouveau document
    collection.update_one(
        {"propertyId": document["propertyId"]},
        {"$set": document},
        upsert=True
    )

    print(f"Data successfully inserted/updated for propertyId '{document['propertyId']}'.")

if __name__ == "__main__":
    sample_run_report("", "")
