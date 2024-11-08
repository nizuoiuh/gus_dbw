import requests
import pandas as pd
import urls
import time
import sys
from utils import progress_bar


def download_variables(page_size: int = 5000,
                       params: dict = {},
                       sleep: float = 0) -> pd.DataFrame:
    return download(url=urls.zmienne,
                    page_size=page_size,
                    params=params,
                    sleep=sleep)


def download_periods(page_size: int = 5000,
                     params: dict = {},
                     sleep: float = 0) -> pd.DataFrame:
    return download(url=urls.okresy,
                    page_size=page_size,
                    params=params,
                    sleep=sleep)


def download_data(
    id: int,
    przekroj: int,
    rok: int,
    okres: int,
    page_size: int = 5000,
    params: dict = {},
    sleep: float = 0,
    print_number_of_requests: bool = False,
    n_lines_up: int = 0,
) -> pd.DataFrame:
    params = dict(
        {
            "id-zmienna": id,
            "id-przekroj": przekroj,
            "id-okres": okres,
            "id-rok": rok
        },
        **params,
    )
    return download(
        url=urls.data,
        page_size=page_size,
        params=params,
        sleep=sleep,
        print_number_of_requests=print_number_of_requests,
        n_lines_up=n_lines_up,
    )


def download_data_for_periods(
    id: int,
    przekroj: int,
    lata: list[int],
    okresy: list[int],
    page_size: int = 5000,
    params: dict = {},
    sleep: float = 0,
) -> pd.DataFrame:
    df = pd.DataFrame()
    liczba_okresow = len(lata) * len(okresy)
    liczba_okresow_zapytanych = 0
    print("Number of periods ", liczba_okresow)
    # setup bar for requests
    progress_bar(0, 1, suffix=" 0/1 1 week requests limit\n")
    for rok in lata:
        for okres in okresy:
            liczba_okresow_zapytanych += 1
            # refresh one line above
            progress_bar(liczba_okresow_zapytanych,
                         liczba_okresow,
                         suffix="# periods\t")
            df = pd.concat([
                df,
                download_data(
                    id=id,
                    przekroj=przekroj,
                    rok=rok,
                    okres=okres,
                    page_size=page_size,
                    params=params,
                    sleep=sleep,
                    print_number_of_requests=True,
                    n_lines_up=1,
                ),
            ])
    df.reset_index(drop=True, inplace=True)
    print("")
    return df


def download(
    url: str,
    page_size: int = 5000,
    params: dict = {},
    sleep: float = 0,
    print_number_of_requests=False,
    n_lines_up: int = 0,
) -> pd.DataFrame:
    page_number = 0
    page_count = 1
    df = pd.DataFrame()
    if "X-ClientId" in params:  # zakladamy ze podany klucz jest poprawny i zwieksza limit :D
        requests_limit = 50000
    else:
        requests_limit = 10000
    while page_number <= page_count:
        parameters = dict(
            {
                "ile-na-stronie": page_size,
                "numer-strony": page_number
            }, **params)
        r = requests.get(url, params=parameters)
        if print_number_of_requests:
            if "X-Rate-Limit-Remaining" in r.headers:
                number_requests_left = int(r.headers["X-Rate-Limit-Remaining"])
                progress_bar(
                    requests_limit - number_requests_left,
                    requests_limit,
                    suffix=
                    f" {requests_limit-number_requests_left}/{requests_limit} 1 week requests limit",
                    linesUp=n_lines_up,
                )
        if sleep > 0:
            time.sleep(sleep)
        if r.status_code == 200:
            page_count = r.json()["page-count"]
            df = pd.concat([df, pd.DataFrame(r.json()["data"])])
            page_number += 1
        elif r.status_code == 404:
            print("Error 404 - Nie znaleziono żądanego zasobu.")
            return pd.DataFrame()
        elif r.status_code == 401:
            print("Error 401 - Niepoprawny klucz API lub klucz wygasł.")
            return pd.DataFrame()
        elif r.status_code == 400:
            print("Error 400 - Serwer nie był w stanie przetworzyć żądania")
            print(r.text)
            return pd.DataFrame()
        elif r.status_code == 429:
            if "Retry-After" in r.headers:
                print("\n####################### Limit reached")
                print("have to wait ", r.headers["Retry-After"])
                wait_time = int(r.headers["Retry-After"])
                for remaining in range(wait_time, 0, -1):
                    sys.stdout.write("\r")
                    sys.stdout.write(
                        "{:2d} seconds remaining.".format(remaining))
                    sys.stdout.flush()
                    time.sleep(1)
            print("\n")
        df.reset_index(drop=True, inplace=True)
    return df
