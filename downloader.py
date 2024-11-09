import requests
import pandas as pd
import urls
import time
import sys
from utils import progress_bar
from cleaner import combine_names_with_data


def download_variable_periods(page_size: int = 5000,
                              params: dict = {},
                              sleep: float = 0) -> pd.DataFrame:
    return download(url=urls.variable_section_periods,
                    page_size=page_size,
                    params=params,
                    sleep=sleep)


def download_variable_data_section(
    id: int,
    section: int,
    year: int,
    period: int,
    page_size: int = 5000,
    params: dict = {},
    sleep: float = 0,
    print_number_of_requests: bool = False,
    n_lines_up: int = 0,
) -> pd.DataFrame:
    params = dict(
        {
            "id-zmienna": id,
            "id-przekroj": section,
            "id-okres": period,
            "id-rok": year
        },
        **params,
    )
    return download(
        url=urls.variable_data_section,
        page_size=page_size,
        params=params,
        sleep=sleep,
        print_number_of_requests=print_number_of_requests,
        n_lines_up=n_lines_up,
    )


def download_variable_section_position(section: int,
                                       page_size: int = 5000,
                                       params: dict = {},
                                       sleep: float = 0) -> pd.DataFrame:
    df = download(urls.variable_section_position,
                  page_size=page_size,
                  params=dict({'id-przekroj': section}, **params),
                  sleep=sleep,
                  print_number_of_requests=True)
    df.reset_index(drop=True, inplace=True)
    return df


def download_data_for_periods(id: int,
                              section: int,
                              years: list[int],
                              periods: list[int],
                              page_size: int = 5000,
                              params: dict = {},
                              sleep: float = 0,
                              get_variable_names: bool = True) -> pd.DataFrame:
    df = pd.DataFrame()
    periods_number = len(years) * len(periods)
    number_periods_requested = 0
    print("Number of periods ", periods_number)
    # setup bar for requests
    progress_bar(0, 1, suffix=" 0/1 1 week requests limit\n")
    for year in years:
        for period in periods:
            number_periods_requested += 1
            # refresh one line above
            progress_bar(number_periods_requested,
                         periods_number,
                         suffix="# periods\t")
            df = pd.concat([
                df,
                download_variable_data_section(
                    id=id,
                    section=section,
                    year=year,
                    period=period,
                    page_size=page_size,
                    params=params,
                    sleep=sleep,
                    print_number_of_requests=True,
                    n_lines_up=1,
                ),
            ])
    df.reset_index(drop=True, inplace=True)
    print("")
    if get_variable_names:
        names = download_variable_section_position(section=section,
                                                   page_size=page_size,
                                                   params=params,
                                                   sleep=sleep)
        df = combine_names_with_data(data=df, names=names)
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
    if "X-ClientId" in params:  # assume that if key parameter is passed limit is higher
        requests_limit = 50000
    else:
        requests_limit = 10000
    while page_number <= page_count:
        parameters = dict(
            params, **{
                "ile-na-stronie": page_size,
                "numer-strony": page_number,
                "lang": "en"
            })
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
            if 'page-count' in r.json():
                page_count = r.json()["page-count"]
            if 'data' in r.json():
                df = pd.concat([df, pd.DataFrame(r.json()["data"])])
            else:
                df = pd.concat([df, pd.DataFrame(r.json())])
            page_number += 1
        elif r.status_code == 404:
            print("\nError 404 - Nie znaleziono żądanego zasobu.")
            return pd.DataFrame()
        elif r.status_code == 401:
            print("\nError 401 - Niepoprawny klucz API lub klucz wygasł.")
            return pd.DataFrame()
        elif r.status_code == 400:
            print("\nError 400 - Serwer nie był w stanie przetworzyć żądania")
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
