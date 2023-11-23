from jobspy import scrape_jobs

jobs = scrape_jobs(
  site_name=["indeed", "linkedin", "zip_recruiter"],
    location="indonesia",
    results_wanted=40,
    country_indeed='indonesia'  # only needed for indeed / glassdoor
)
print(f"Found {len(jobs)} jobs")
print(jobs.head())

jobs.to_csv("Job_Vacant.csv", index=False) # to_xlsx