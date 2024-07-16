import datetime
import requests
import csv
import os
from getpass import getpass


interval_minutes = 60
days = 7
file = "dnsDashboard.csv"

head = ["domainId", "domainName", "from", "to", "numOfRequestsTotalPeak", "numOfRequestsTotalAverage",
        "numOfRequestsPassedPeak", "numOfRequestsPassedAverage", "numOfRequestsBlockedPeak",
        "numOfRequestsBlockedAverage", "numberOfRequestsCachedPeak",
        "numberOfRequestsCachedAverage", "numberOfTotalRequests", "numberOfTotalBlockRequests",
        "numberOfRequestsCached", "numberOfRequestsPassedToOrigin"]


def gerar_timestamps():
    dateNow = datetime.datetime.now()
    dateNow = dateNow.replace(minute=0, second=0, microsecond=0)
    delta = datetime.timedelta(minutes=interval_minutes)
    start = dateNow - datetime.timedelta(days=days)

    timestamps = []
    while start <= dateNow:
        timestamp_unix = int(start.timestamp() * 1000)
        timestamps.append(timestamp_unix)
        start += delta

    return timestamps


def statistics(domains, fromTime, toTime):
    url = f'https://api.imperva.com/dns/v3/domains/statistics'

    params = {
        'domainIds': f'{str(domains[0]).strip('[]')}',
        'from': f'{fromTime}',
        'to': f'{toTime}'
    }

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 202:
        return parseStatistics(response.json()['data'], domains[1])
    else:
        return (f"Request error: {response.text}")


def parseStatistics(data, domainName):
    try:

        formated = {
            "domainId": data[0]['domainIds'][0],
            "domainName": domainName[0],
            "from": data[0]['from'],
            "to": data[0]['to'],
            "numOfRequestsTotalPeak": round(data[0]['numOfRequestsTotalPeak'], 2),
            "numOfRequestsTotalAverage": round(data[0]['numOfRequestsTotalAverage'], 2),
            "numOfRequestsPassedPeak": round(data[0]['numOfRequestsPassedPeak'], 2),
            "numOfRequestsPassedAverage": round(data[0]['numOfRequestsPassedAverage'], 2),
            "numOfRequestsBlockedPeak": round(data[0]['numOfRequestsBlockedPeak'], 2),
            "numOfRequestsBlockedAverage": round(data[0]['numOfRequestsBlockedAverage'], 2),
            "numberOfRequestsCachedPeak": round(data[0]['numberOfRequestsCachedPeak'], 2),
            "numberOfRequestsCachedAverage": round(data[0]['numberOfRequestsCachedAverage'], 2),
            "numberOfTotalRequests": round(data[0]['numberOfTotalRequests'], 2),
            "numberOfTotalBlockRequests": round(data[0]['numberOfTotalBlockRequests'], 2),
            "numberOfRequestsCached": round(data[0]['numberOfRequestsCached'], 2),
            "numberOfRequestsPassedToOrigin": round(data[0]['numberOfRequestsPassedToOrigin'], 2)
        }
    except:
        print(data)

    return formated


def allDomains():
    url = 'https://api.imperva.com/dns/v3/domains'

    response = requests.get(url, headers=headers)

    domains = []

    for domain in response.json()['data']:
        domains.append([domain['id'], [domain['name']]])

    return domains


def writeFile(file_path):
    timestamps = gerar_timestamps()

    with open(file_path, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=head)
        writer.writeheader()

        for domain in domains:
            print(f"This process takes some time. Please wait for the script to finish executing. | Obtaining data from: {
                  domain[1][0]}")
            for ts in range(1, len(timestamps)):
                start = timestamps[ts-1]
                end = timestamps[ts]

                try:
                    data = statistics(domain, start, end)
                    writer.writerow(data)
                except:
                    print(data)

                csvfile.flush()


def appendFile(file_path):
    try:
        with open(file_path, 'r') as csvfile:
            reader = csv.reader(csvfile)
            lines = list(reader)
            last_line = lines[-1]
            last_timestamp = int(last_line[3])
    except FileNotFoundError:
        print(f"File not found. Please make sure '{
              file}' exists in the directory.")
        return

    timestamps = gerar_timestamps()
    timestamps = [ts for ts in timestamps if ts > last_timestamp]

    if not timestamps:
        print("No new data to append.")
        return

    with open(file_path, 'a', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=head)

        for domain in domains:
            print(f"This process takes some time. Please wait for the script to finish executing. | Obtaining data from: {
                  domain[1][0]}")
            for ts in range(1, len(timestamps)):
                start = timestamps[ts-1]
                end = timestamps[ts]

                try:
                    data = statistics(domain, start, end)
                    writer.writerow(data)
                except Exception as e:
                    print(f"An error occurred while processing data: {e}")

                csvfile.flush()


def credits():

    credit_text = """
    **************************************************
    *                                                *
    *          CLOUD WAF - DNS PROTECTION            *
    *               DATA for POWERBI                 *
    *                                                *
    *  Script developed by: Gustavo Prates           *
    *  Contact: gprates@eteknovared.com.br           *
    *                                                *
    *  Owned by: Novared Brasil Ltda.                *
    *                                                *
    **************************************************
    """

    explanation_text = """
    Script designed to generate a .csv file containing data from DNS Protection of Imperva Cloud WAF.
    
    You will need to install the 'requests' library:
    
        pip install requests
     
    """

    print(credit_text)
    print(explanation_text)


if __name__ == '__main__':
    try:
        credits()

        apiid = input("Insert API ID: ")
        apikey = getpass('Insert API KEY: ')

        print()

        headers = {
            'accept': 'application/json',
            'x-API-Key': f'{apikey}',
            'x-API-Id': f'{apiid}',
            'Content-Type': 'application/json',
        }

        try:
            domains = allDomains()
        except:
            print("Invalid credentials\n")
            quit()

        file_dir = input(
            r"Enter the directory path for '{}': ".format(file)).strip()
        file_path = os.path.join(file_dir, file)

        options = int(input(f"""
Options:

    [1] - New file
    [2] - From last data of {file} to now
    [3] - Close

Choose an option: """))

        if options == 1:
            writeFile(file_path)
        elif options == 2:
            appendFile(file_path)
        else:
            quit()
    except KeyboardInterrupt:
        print("\n\nExecution interrupted by user. Exiting...")
        quit()
