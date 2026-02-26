SPREADSHEET_ID = "10vUOhRr7IAXlRrkBphxEP4ApXYBgrnuxJq6G83GnfHI"

GID_CAFES = {
    "Mar del Plata": "0",
    "Buenos Aires": "1296176686",
    "La Plata": "208452991",
    "Córdoba": "1250014567",
    "Rosario": "1691979590",
}

GID_TOSTADORES = "1590442133"

def sheet_url(gid: str) -> str:
    return (
        f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}"
        f"/gviz/tq?tqx=out:csv&gid={gid}"
    )

CACHE_TTL = 600
