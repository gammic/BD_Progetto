import requests
import time
from datetime import datetime
import matplotlib.pyplot as plt


def main():
    n_times = 3
    u = start_connection()
    t1 = n_rec_totali_couch(u, n_times)
    t2 = most_rec_p_couch(u, n_times)
    t3 = most_rec_u_couch(u, n_times)
    t4 = dist_val_couch(u, n_times)
    t5 = hi_utility_rec_couch(u, n_times)
    t6 = hi_val_prods_couch(u, n_times)
    t7 = temp_trend_couch(u, n_times)
    t8 = len_rec_val_couch(u, n_times)
    show_times([t1, t2, t3, t4, t5, t6, t7, t8])


def start_connection():
    base_url = "http://gammic:marco2002@127.0.0.1:5984/amazon_reviews"
    print("Connessione stabilita al DB CouchDB")
    return base_url


def n_rec_totali_couch(url, n):
    times = []

    for x in range(n):
        start = time.time()
        response = requests.get(url)
        doc_count = response.json()["doc_count"]
        end = time.time()
        times.append(end - start)
    print(f"Numero di recensioni : {doc_count}")
    return sum(times) / len(times)


def most_rec_p_couch(base_url, n):
    times = []
    for x in range(1, n + 1):
        start = time.time()
        view_url = f"{base_url}/_design/most_reviewed_products/_view/most_reviewed_products?reduce=true&group=true"
        response = requests.get(view_url)
        if response.status_code == 200:
            print(f"Richiesta {x} andata a buon fine")
        else:
            print(f"Richiesta {x} non andata a buon fine con codice {response.status_code}")
        results = response.json()["rows"]
        sorted_results = sorted(results, key=lambda x: x["value"], reverse=True)[:10]
        end = time.time()
        times.append(end - start)

    print("10 prodotti più recensiti")
    for item in sorted_results:
        print(f"Prodotto {item['key']}: {item['value']} recensioni")
    return sum(times) / len(times)


def most_rec_u_couch(base_url, n):
    times = []
    for x in range(1, n + 1):
        start = time.time()
        view_url = f"{base_url}/_design/most_reviewed_users/_view/most_reviewed_users?reduce=true&group=true"
        response = requests.get(view_url)
        if response.status_code == 200:
            print(f"Richiesta {x} andata a buon fine")
        else:
            print(f"Richiesta {x} non andata a buon fine con codice {response.status_code}")

        results = response.json()["rows"]
        sorted_results = sorted(results, key=lambda x: x["value"], reverse=True)[:10]
        end = time.time()
        times.append(end - start)

    print("10 utenti con più recensioni")
    for item in sorted_results:
        print(f"Utente {item['key']}: {item['value']} recensioni")
    return sum(times) / len(times)


def dist_val_couch(base_url, n):
    times = []
    for x in range(1, n + 1):
        start = time.time()
        view_url = f"{base_url}/_design/distribution_of_scores/_view/distribution_of_scores?reduce=true&group=true"
        response = requests.get(view_url)
        if response.status_code == 200:
            print(f"Richiesta {x} andata a buon fine")
        else:
            print(f"Richiesta {x} non andata a buon fine con codice {response.status_code}")

        results = response.json()["rows"]
        end = time.time()
        times.append(end - start)

    print("Distribuzione delle valutazioni")
    for item in results:
        print(f"Recensioni con {item['key']} stelle: {item['value']}")
    return sum(times) / len(times)


def hi_utility_rec_couch(base_url, n):
    times = []
    for x in range(1, n + 1):
        start = time.time()
        view_url = f"{base_url}/_design/high_utility_reviews/_view/high_utility_reviews?reduce=false"
        response = requests.get(view_url)
        if response.status_code == 200:
            print(f"Richiesta {x} andata a buon fine")
        else:
            print(f"Richiesta {x} non andata a buon fine con codice {response.status_code}")

        results = response.json()["rows"]

        for item in results:
            item['value'][0] = item['value'][0] / item['value'][1] if item['value'][1] != 0 else 0
            if item['value'][0] > 1.00:
                results.remove(item)
        sorted_results = sorted(results, key=lambda x: x['value'][0], reverse=True)[:10]
        end = time.time()
        times.append(end - start)

    print("Recensioni più utili")
    for item in sorted_results:
        print(f"ProductID: {item['key'][0]}")
        print(f"UserID: {item['key'][1]}")
        print(f"HelpfulnessRatio: {item['value'][0]:.2f}")
        print(f"Summary: {item['value'][2]}")
        print("-" * 60)

    return sum(times) / len(times)


def hi_val_prods_couch(base_url, n):
    times = []
    for x in range(1, n + 1):
        start = time.time()
        view_url = f"{base_url}/_design/products_with_avg_score/_view/products_with_avg_score?reduce=true&group=true"
        response = requests.get(view_url)
        if response.status_code == 200:
            print(f"Richiesta {x} andata a buon fine")
        else:
            print(f"Richiesta {x} non andata a buon fine con codice {response.status_code}")

        results = response.json()["rows"]
        for item in results:
            item['value']['sum'] = item['value']['sum'] / item['value']['count']
        filtered_results = [item for item in results if item['value']['count'] >= 50]

        sorted_results = sorted(filtered_results, key=lambda x: x['value']['sum'], reverse=True)[:10]
        end = time.time()
        times.append(end - start)

    print("Prodotti con valutazione media maggiore")
    for item in sorted_results:
        print(f"Prodotto {item['key']}: Media {item['value']['sum']}, Recensioni {item['value']['count']}")

    return sum(times) / len(times)


def temp_trend_couch(base_url, n):
    times = []
    for x in range(1, n + 1):
        start = time.time()
        view_url = f"{base_url}/_design/temp_trend/_view/temp_trend?reduce=true&group=true"
        response = requests.get(view_url)
        if response.status_code == 200:
            print(f"Richiesta {x} andata a buon fine")
        else:
            print(f"Richiesta {x} non andata a buon fine con codice {response.status_code}")

        results = response.json()["rows"]

        dates = [datetime.strptime(item['key'], '%Y-%m-%d') for item in results]
        counts = [item['value'] for item in results]

        sorted_data = sorted(zip(dates, counts))
        dates_sorted, counts_sorted = zip(*sorted_data)

        end = time.time()
        times.append(end - start)

    plt.figure(figsize=(14, 6))
    plt.plot(dates_sorted, counts_sorted, color='blue')
    plt.title("Trend temporale delle recensioni")
    plt.xlabel("Data")
    plt.ylabel("Numero di recensioni")
    plt.grid(True)
    plt.tight_layout()
    plt.show()

    return sum(times) / len(times)


def len_rec_val_couch(base_url, n):
    times = []
    for x in range(1, n + 1):
        start = time.time()
        view_url = f"{base_url}/_design/review_length_by_score/_view/review_length_by_score?reduce=true&group=true"
        response = requests.get(view_url)
        if response.status_code == 200:
            print(f"Richiesta {x} andata a buon fine")
        else:
            print(f"Richiesta {x} non andata a buon fine con codice {response.status_code}")

        results = response.json()["rows"]

        print("Lunghezza recensione per valutazione")
        for item in results:
            print(
                f"Valutazione {item['key']} stelle: {(item['value']['total'] / item['value']['count']):.2f} caratteri medi")

        end = time.time()
        times.append(end - start)

    return sum(times) / len(times)


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
    plt.title("Tempi di esecuzione delle query CouchDB")
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()

    for bar in bars:
        yval = bar.get_height()
        plt.text(bar.get_x() + bar.get_width() / 2, yval + 0.01, f"{yval:.2f}", ha='center', va='bottom')

    plt.show()
    plt.savefig("tempi_exec_couch.png")



if __name__ == "__main__":
    main()
