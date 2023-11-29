from jobspy import scrape_jobs
import pandas as pd
from sqlalchemy import create_engine
import time

def scrape_job():
    keyword = ['Teacher',
    'Nurse',
    'Marketing Manager',
    'Accountant',
    'Graphic Designer',
    'Sales Executive',
    'Customer Service',
    'Pharmacist',
    'Chef',
    'Police Officer',
    'Journalist',
    'Dentist',
    'Real Estate',
    'Mechanic',
    'Flight Attendant',
    'Social Media',
    'Bank Teller',
    'Tour Guide',
    'Interior Designer',
    'Translator',
    'Event Planner',
    'Librarian',
    'Security Guard',
    'Market Research',
    'Fashion',
    'Pharmacy',
    'Photographer',
    'Fitness Instructor',
    'Travel Agent',
    'Scientist',
    'Archivist',
    'Veterinarian']

    job_data_list = []  # Initialize an empty list to accumulate job data

    for term in keyword:
        df_job = scrape_jobs(
            site_name=["indeed", "linkedin"],
            search_term=term,
            location="indonesia",
            results_wanted=30,
            country_indeed='indonesia'  # only needed for indeed / glassdoor
        )

        job_data_list.append(df_job)  # Append the DataFrame to the list

        # Introduce a delay of 30 seconds
        time.sleep(30)

    # Concatenate all DataFrames in the list into a single DataFrame
    result_df = pd.concat(job_data_list, ignore_index=True)

    return result_df


def transform_job(df_job):

  #ubah nama provinsi
  #replace jd jawa barat
  df_job['location'].replace(['Bandung, JB, Indonesia','Bekasi, JB, Indonesia','Depok, JB, Indonesia','JB, Indonesia','JB, Indonesia','Karawang, JB, Indonesia'], 'Jawa Barat', inplace=True)

  #replace jd jawa tengah
  df_job['location'].replace(['Salatiga, JT, Indonesia','Surakarta, JT, Indonesia'], 'Jawa Tengah', inplace=True)

  #replace jadi di yogyakarta
  df_job['location'].replace(['Bantul, YO, Indonesia','Yogyakarta, YO, Indonesia','Yogyakarta, Indonesia'], 'DI Yogyakarta', inplace=True)

  #replace jadi jawa timur
  df_job['location'].replace(['Gresik, JI, Indonesia','JI, Indonesia','Malang, JI, Indonesia','Surabaya, JI, Indonesia'], 'Jawa Timur', inplace=True)

  #replace jadi bali
  df_job['location'].replace(['Bali, Indonesia','Badung, BA, Indonesia','BA, Indonesia','Denpasar, BA, Indonesia','Kuta, BA, Indonesia'], 'Bali', inplace=True)

  #replace jadi kalimantan timur
  df_job['location'].replace(['Balikpapan, KI, Indonesia'], 'Kalimantan Timur', inplace=True)

  #replace jadi kalimantan selatan
  df_job['location'].replace(['Banjarmasin, KS, Indonesia','KS, Indonesia'], 'Kalimantan Selatan', inplace=True)

  #replace jadi kepulauan riau
  df_job['location'].replace(['Batam, KR, Indonesia'], 'Kepulauan Riau', inplace=True)

  #replace jadi dki jakarta
  df_job['location'].replace(['Jakarta, Indonesia','Jakarta, JK, Indonesia'], 'DKI Jakarta', inplace=True)

  #replace jd lampung
  df_job['location'].replace(['LA, Indonesia'], 'Lampung', inplace=True)

  #replace jd Banten
  df_job['location'].replace(['Tangerang, BT, Indonesia'], 'Banten', inplace=True)


  #drop unrelated columns & rows containing NaN
  df_job = df_job.dropna(subset=['location'])
  df_job = df_job[['site','title', 'company', 'location', 'description']]

  return df_job

def load_job(df_job):
  engine = create_engine('postgresql://postgres:RekdatETLTimP@20.127.61.149:5432/postgres')

  df_job.to_sql('job_new', engine, if_exists='append', index=False)

job = scrape_job()
transformed_job = transform_job(job)
load_job(transformed_job)