import httpx

def get_pypi_api(techname: str):
    url = f"https://pypistats.org/api/packages/{techname}/recent"
    response = httpx.get(url)
    data_pypi = response.json()
    print("python done")
    return data_pypi