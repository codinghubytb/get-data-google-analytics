import json
from datetime import datetime, timedelta
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from google.oauth2 import service_account
from pymongo import MongoClient
from collections import defaultdict

# Configuration MongoDB
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = ""
COLLECTION_NAME = ""
SERVICE_ACCOUNT_FILE = ".json"

def sample_run_report(property_id, source_name):
    credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
    client = BetaAnalyticsDataClient(credentials=credentials)

    # Génération des mois de janvier 2024 à janvier 2025
    start_date = datetime.strptime("2024-01-01", "%Y-%m-%d")
    end_date = current_date = datetime.now()

    monthly_results = {}
    temp_date = start_date

    while temp_date <= end_date:
        month_start = temp_date
        month_end = (temp_date + timedelta(days=32)).replace(day=1) - timedelta(days=1)  # Dernier jour du mois
        if month_end > end_date:
            month_end = end_date

        # Requête pour le mois courant
        request = RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=[Dimension(name="date")],
            metrics=[Metric(name="activeUsers")],
            date_ranges=[DateRange(
                start_date=month_start.strftime("%Y-%m-%d"),
                end_date=month_end.strftime("%Y-%m-%d")
            )],
        )
        response = client.run_report(request)

        # Calcul du total pour le mois
        total_users = sum(int(row.metric_values[0].value) for row in response.rows)
        month_label = month_start.strftime('%b %Y')  # Format "Jan 2024"
        monthly_results[month_label] = total_users

        temp_date = (temp_date + timedelta(days=32)).replace(day=1)  # Passer au mois suivant

    # Calcul des 30 derniers jours
    current_date = datetime.now()
    date_30_days_ago = (current_date - timedelta(days=30)).strftime('%Y-%m-%d')
    request2 = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name="date")],
        metrics=[Metric(name="activeUsers")],
        date_ranges=[DateRange(
            start_date=date_30_days_ago,
            end_date=current_date.strftime("%Y-%m-%d")
        )],
    )
    response2 = client.run_report(request2)
    nb_user_30_days = sum(int(row.metric_values[0].value) for row in response2.rows)

    # Préparer le document MongoDB
    document = {
        "propertyId": property_id,
        "source": source_name,
        "data": [{"month": month, "activeUsers": monthly_results[month]} for month in monthly_results],
        "nbUser30Day": nb_user_30_days
    }

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
