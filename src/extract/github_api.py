import httpx
import json

def get_github_metrics(techname: str):
    repo_url= f"https://api.github.com/repos/{techname}"
    response = httpx.get(repo_url)
    git_data = response.json()
    print("git done")
    return git_data