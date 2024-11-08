import sys
from downloader import pobierz_dane_dla_okresow

# import requests
from cleaner import szereg_czasowy_cpi_ogolem


def main() -> int:
    # warning from ignoring ssl certificates :)
    # requests.packages.urllib3.disable_warnings()
    okresy = []
    okresy.extend(range(247, 259))  # miesiace, od stycznia do grudnia, 247-258
    lata = []
    lata.extend(range(2023, 2024))
    # zmienne = pobierz_zmienne()
    df = pobierz_dane_dla_okresow(
        id=305,
        przekroj=736,
        lata=lata,
        okresy=okresy,
        page_size=5000,
        params={},
        sleep=1,
    )
    df = szereg_czasowy_cpi_ogolem(df)
    print(df)
    return 0


if __name__ == "__main__":
    sys.exit(main())
