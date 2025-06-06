import time
from datetime import datetime

import matplotlib.pyplot as plt
from pymongo import MongoClient


def main():
    c = start_connection()
    t1 = n_rec_totali(c)
    t2 = most_rec_p(c)
    t3 = most_rec_u(c)
    t4 = dist_val(c)
    t5 = hi_utility_rec(c)
    t6 = hi_val_prods(c)
    t7 = temp_trend(c)
    t8 = len_rec_val(c)
    show_times([t1, t2, t3, t4, t5, t6, t7, t8])


def start_connection():
    client = MongoClient("mongodb://localhost:27017/")
    print("Connessione stabilita al DB")
    db = client["amazonDB"]
    collection = db["reviews"]
    return collection


def n_rec_totali(collection):
    # Conteggio recensioni totali
    times=[]
    for x in range(3):
        start = time.time()
        n_rec = collection.count_documents({})
        end = time.time()
        times.append(end - start)
    print(f"Numero di recensioni : {n_rec}")
    return sum(times)/len(times)


def most_rec_p(collection):
    # 10 prodotti più recensiti
    times=[]
    for x in range(3):
        start = time.time()
        most_rec_prods = collection.aggregate([
            {"$group": {"_id": "$ProductId", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 10}
        ])
        end = time.time()
        times.append(end - start)
    print("10 prodotti più recensiti")
    for product in most_rec_prods:
        print(f"Prodotto {product['_id']}: {product['count']} recensioni")
    return sum(times)/len(times)


def most_rec_u(collection):
    # 10 user con più recensioni
    times=[]
    for x in range(3):
        start = time.time()
        usr_most_rec = collection.aggregate([
            {"$group": {"_id": "$ProfileName", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 10}
        ])
        end = time.time()
        times.append(end - start)
    print("10 user con più recensioni")
    for user in usr_most_rec:
        print(f"User {user['_id']}: {user['count']} recensioni")
    return sum(times)/len(times)


def dist_val(collection):
    # Distribuzione delle valutazioni
    times = []
    for x in range(3):
        start = time.time()
        distr_val = collection.aggregate([
            {"$group": {"_id": "$Score", "count": {"$sum": 1}}},
            {"$sort": {"_id": -1}},
        ])
        end = time.time()
        times.append(end - start)
    print("Distribuzione delle valutazioni")
    for val in distr_val:
        print(f"Recensioni con {val['_id']} stelle: {val['count']}")
    return sum(times)/len(times)


def hi_utility_rec(collection):
    # Recensioni con alta utilità
    pipeline = [
        {
            "$match": {
                "HelpfulnessDenominator": {"$gt": 0}
            }
        },
        {
            "$addFields": {
                "HelpfulnessRatio": {
                    "$divide": ["$HelpfulnessNumerator", "$HelpfulnessDenominator"]
                }
            }
        },
        {
            "$match": {
                "HelpfulnessRatio": {"$lte": 1}
            }
        },
        {
            "$sort": {
                "HelpfulnessRatio": -1
            }
        },
        {
            "$limit": 10
        }
    ]
    times = []
    for x in range(3):
        start = time.time()
        top_rec = collection.aggregate(pipeline)
        end = time.time()
        times.append( end - start)
    print("Recensioni più utili")
    for rec in top_rec:
        print(f"ProductID: {rec['ProductId']}")
        print(f"UserID: {rec['UserId']}")
        print(f"Ratio: {rec['HelpfulnessRatio']:.2f}")
        print(f"Summary: {rec['Summary']}")
        print("-" * 60)
    return sum(times)/len(times)


def hi_val_prods(collection):
    # Prodotti con media maggiore
    times = []
    for x in range(3):
        start = time.time()
        avg_score = collection.aggregate([
            {"$group": {
                "_id": "$ProductId",
                "avgScore": {"$avg": "$Score"},
                "count": {"$sum": 1}
            }},
            {"$match": {"count": {"$gte": 50}}},  # solamente prodotti con almeno 50 recensioni
            {"$sort": {"avgScore": -1}},
            {"$limit": 10}
        ])
        end = time.time()
        times.append(end - start)
    print("Prodotti con valutazione media maggiore")
    for product in avg_score:
        print(f"Prodotto {product['_id']} ha valutazione media {product['avgScore']}")
    return sum(times)/len(times)


def temp_trend(collection):
    # Trend temporale delle recensioni
    pipeline = [
        {
            "$addFields": {
                "date": {
                    "$dateToString": {
                        "format": "%Y-%m-%d",
                        "date": {"$toDate": {"$multiply": ["$Time", 1000]}}
                    }
                }
            }
        },
        {
            "$group": {
                "_id": "$date",
                "count": {"$sum": 1}
            }
        },
        {
            "$sort": {
                "_id": 1
            }
        }
    ]
    times = []
    for x in range(3):
        start = time.time()
        trend_giornaliero = list(collection.aggregate(pipeline))
        end = time.time()
        times.append(end - start)
    dates = [datetime.strptime(r['_id'], '%Y-%m-%d') for r in trend_giornaliero]
    counts = [r['count'] for r in trend_giornaliero]

    plt.figure(figsize=(14, 6))
    plt.plot(dates, counts, color='blue')
    plt.title("Trend temporale delle recensioni")
    plt.xlabel("Data")
    plt.ylabel("Numero di recensioni")
    plt.grid(True)
    plt.tight_layout()
    plt.show()
    return sum(times)/len(times)


def len_rec_val(collection):
    # Lunghezza recensione per valutazione
    pipeline = [
        {"$addFields": {
            "textLength": {"$strLenCP": "$Text"}
        }},
        {"$group": {
            "_id": "$Score",
            "avgLength": {"$avg": "$textLength"}
        }},
        {"$sort": {"_id": 1}}
    ]

    print("Lunghezza testo recensione per valutazione")
    times = []
    for x in range(3):
        start = time.time()
        len_text = collection.aggregate(pipeline)
        end = time.time()
        times.append(end - start)
    for l in len_text:
        print(f"{l['_id']} stelle: {l['avgLength']} parole")
    return sum(times)/len(times)


def show_times(t):
    query_labels = [
        "Totale recensioni",
        "Prodotti più recensiti",
        "Utenti con più recensioni",
        "Distribuzione valutazioni",
        "Recensioni utili",
        "Prodotti migliori",
        "Trend temporale delle recensioni",
        "Lunghezza testo recensioni"
    ]
    plt.figure(figsize=(10, 6))
    bars = plt.bar(query_labels, t, color='red')
    plt.xlabel("Query")
    plt.ylabel("Tempo di esecuzione (s)")
    plt.title("Tempi di esecuzione delle query MongoDB")
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()

    for bar in bars:
        yval = bar.get_height()
        plt.text(bar.get_x() + bar.get_width() / 2, yval + 0.01, f"{yval:.2f}", ha='center', va='bottom')
    plt.savefig("tempi_exec_mongo.png")
    plt.show()


if __name__ == "__main__":
    main()
