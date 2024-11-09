import sys

# import requests
from downloader import download_data_for_periods
from cleaner import szereg_czasowy_cpi_ogolem


def main() -> int:
    okresy = []
    okresy.extend(range(247, 259))  # miesiace, od stycznia do grudnia, 247-258
    lata = []
    lata.extend(range(2023, 2024))
    #df = download_variable_section_position(736)
    df = download_data_for_periods(id=305,
                                   section=736,
                                   years=lata,
                                   periods=okresy,
                                   page_size=5000,
                                   params={},
                                   sleep=1,
                                   get_variable_names=True)
    print(df)
    df = szereg_czasowy_cpi_ogolem(df)
    print(df)
    return 0


if __name__ == "__main__":
    sys.exit(main())
