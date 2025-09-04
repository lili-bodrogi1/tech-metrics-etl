import httpx

def get_pypi_api(techname: str):
    name = techname.split('/')[-1]
    url = f"https://pypistats.org/api/packages/{name.lower()}/recent"
    response = httpx.get(url)
    data_pypi = response.json()
    print("python done", name)
    return data_pypi