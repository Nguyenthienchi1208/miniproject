import requests, random, time, csv
from bs4 import BeautifulSoup
from datetime import datetime

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.5845.96 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
]

CATEGORY_URLS = [
    "https://www.topcv.vn/tim-viec-lam-nha-hang-dich-vu-an-uong-cr857cb858?type_keyword=1&sba=1&category_family=r177~b179-r544~b550-r711~b716-r857~b858_b859"
]

def get_jobs(soup):
    jobs = []
    for job in soup.select("div.job-item-search-result"):
        title_tag = job.select_one("h3.title a")
        title = title_tag.text.strip() if title_tag else None
        link = title_tag["href"] if title_tag and title_tag.has_attr("href") else None
        company = job.select_one("a.company")
        salary = job.select_one("label.title-salary")
        address = job.select_one("label.address")
        posted = job.select_one("label.label-update")

        jobs.append({
            "created_date": datetime.now().strftime("%Y-%m-%d"),
            "job_title": title,
            "company": company.text.strip() if company else None,
            "salary": salary.text.strip() if salary else None,
            "address": address.text.strip() if address else None,
            "time": posted.text.strip() if posted else None,
            "link_description": link
        })
    return jobs

def crawl_category(url, max_pages=60):
    data = []
    for page in range(1, max_pages + 1):
        headers = {"User-Agent": random.choice(USER_AGENTS), "Referer": "https://www.google.com/"}
        try:
            res = requests.get(f"{url}&page={page}", headers=headers, timeout=15)
            res.raise_for_status()
        except Exception as e:
            print(f"[ERR] page {page}: {e}")
            break

        soup = BeautifulSoup(res.content, "html.parser")
        jobs = get_jobs(soup)
        if not jobs:
            print(f"Stop at page {page}")
            break

        print(f"Page {page}: {len(jobs)} jobs")
        data.extend(jobs)
        time.sleep(random.uniform(1.5, 3.5))
    return data

def main():
    all_jobs = []
    for url in CATEGORY_URLS:
        print("Crawling:", url)
        all_jobs.extend(crawl_category(url, max_pages=60))

    if all_jobs:
        with open("topcv3_jobs.csv", "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=all_jobs[0].keys())
            writer.writeheader()
            writer.writerows(all_jobs)
        print(len(all_jobs))

if __name__ == "__main__":
    main()
