import sys

# import requests
from downloader import download_data_for_periods
from cleaner import szereg_czasowy_cpi_ogolem,create_time_series

def main() -> int:
    okresy = []
    okresy.extend(range(247, 259))  # miesiace, od stycznia do grudnia, 247-258
    lata = []
    lata.extend(range(2022, 2024))
    #df = download_variable_section_position(736)
    df = download_data_for_periods(id=305,
                                   section=736,
                                   years=lata,
                                   periods=okresy,
                                   page_size=5000,
                                   params={},
                                   sleep=0.2,
                                   get_variable_names=True)
    print(df)
    df= create_time_series(df)
    print(df)
    print(df.iloc[:,(df.columns.get_level_values(1)=='milk')])
    return 0


if __name__ == "__main__":
    sys.exit(main())
