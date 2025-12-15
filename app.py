import time
import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
from datetime import datetime, timedelta
import random
import os

# ============================
# CONFIGURATION
# ============================

# Get yesterday's date (or today's, depending on preference)
# Using today for filename clarity in automation
current_date = datetime.now()
date_str = current_date.strftime('%Y-%m-%d')

countries = ["Sub-Saharan Africa", "Northern Africa", "Eastern Africa", "Western Africa", "Southern Africa"]
excluded_countries = ["Spain"] 

# General search (no specific keywords)
keywords = [""] 

# Robust headers
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9,fr;q=0.8",
    "Referer": "https://www.google.com/"
}

# ============================
# HELPER FUNCTIONS
# ============================

def get_with_retry(url, retries=3):
    for i in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                return response
            elif response.status_code == 429:
                print(f"   Rate limited. Sleeping for {10 * (i+1)} seconds...")
                time.sleep(10 * (i+1))
            else:
                time.sleep(2)
        except Exception as e:
            print(f"   Connection error: {e}")
            time.sleep(2)
    return None

def extract_education(description):
    if not description or description == "N/A":
        return "N/A"
    
    desc_lower = description.lower()
    education_map = {
        "Bac+8 / Doctorat": ["bac+8", "doctorat", "phd", "doctorate", "dba"],
        "Bac+5 / Master / Ingénieur": ["bac+5", "master", "msc", "mba", "ingénieur", "ingenieur", "engineer", "dea", "dess"],
        "Bac+4": ["bac+4", "maîtrise", "m1"],
        "Bac+3 / Licence": ["bac+3", "licence", "bachelor", "license", "graduate"],
        "Bac+2 / BTS / DUT": ["bac+2", "bts", "dut", "deug", "technicien supérieur", "associate degree"],
        "Bac": ["bac", "niveau bac", "baccalauréat", "high school diploma"],
        "Certified": ["certified", "certification", "certificat", "certifiée", "certifié"]
    }

    for level, keywords_list in education_map.items():
        for k in keywords_list:
            if re.search(r'\b' + re.escape(k) + r'\b', desc_lower) or k in desc_lower:
                return level
    return "N/A"

def extract_experience(description):
    if not description or description == "N/A":
        return "N/A"

    desc_lower = description.lower()
    patterns = [
        r"(\d+)\s*(?:à|au|to|-)\s*\d+\s*(?:ans|years|années)",
        r"(?:plus de|over|more than|min|minimum|au moins|environ|at least)\s*(\d+)\s*(?:ans|years|années)",
        r"(\d+)\s*(?:ans|years|années)\s*(?:d'|of)?\s*(?:expérience|experience)",
        r"(?:at least|min|minimum)\s*(\d+)\s*(?:ans|years|années)?\s*(?:of\s*)?(?:expérience|experience)",
        r"(\d+)\s*\+?\s*(?:ans|years|années)"
    ]
    
    text_numbers = {
        "un": "1", "one": "1", "deux": "2", "two": "2", "trois": "3", "three": "3", 
        "quatre": "4", "four": "4", "cinq": "5", "five": "5", "six": "6", "seven": "7",
        "huit": "8", "neuf": "9", "dix": "10"
    }

    for p in patterns:
        match = re.search(p, desc_lower)
        if match:
            result = match.group(1).replace(" ", "")
            return f"{result} ans/years"

    for word, number in text_numbers.items():
        if f"{word} ans" in desc_lower or f"{word} years" in desc_lower:
            return f"{number} ans/years"
            
    if "senior" in desc_lower or "sénior" in desc_lower:
        return "Senior (> 5 ans)"
    if "junior" in desc_lower or "débutant" in desc_lower:
        return "Junior (0-2 ans)"
    if "confirmé" in desc_lower:
        return "Confirmé (3-5 ans)"

    return "N/A"

# ============================
# STEP 1 — SCRAPE LISTINGS
# ============================

links = []
api_links = []
seen_urls = set()

print("=== Starting Step 1: Harvesting Links ===")

for country in countries:
    for keyword in keywords:
        display_kw = "General Search" if keyword == "" else keyword
        print(f"Searching: {display_kw} in {country}")
        
        empty_page_count = 0 
        
        # Max 50 pages per country/keyword to keep runtime reasonable for GitHub Actions
        for page in range(0, 50): 
            if empty_page_count > 2:
                break

            url = (
                f"https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?"
                f"keywords={keyword}&location={country}&f_TPR=r86400&start={page*25}"
            )

            response = get_with_retry(url)

            if response:
                soup = BeautifulSoup(response.text, "html.parser")
                job_links = soup.find_all("a", class_="base-card__full-link")
                
                if not job_links:
                    print(f"   No jobs found on page {page}. Stopping this location.")
                    empty_page_count += 1
                    continue
                else:
                    empty_page_count = 0 

                for job in job_links:
                    job_url = job.get("href")
                    if job_url:
                        clean_url = job_url.split('?')[0]
                        
                        if clean_url not in seen_urls:
                            seen_urls.add(clean_url)
                            links.append((clean_url, keyword))

                            match = re.search(r'-(\d+)', job_url)
                            if match:
                                job_id = match.group(1)
                                api_url = f"https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}"
                                api_links.append((api_url, keyword, clean_url))
            
            time.sleep(random.uniform(1, 2))

print(f"Total unique jobs found: {len(api_links)}")

# ============================
# STEP 2 — SCRAPE DETAILS
# ============================

data = []

print("\n=== Starting Step 2: Extracting Details ===")

for i, (api_url, searched_keyword, original_link) in enumerate(api_links):
    print(f"Processing {i+1}/{len(api_links)}", end="\r")
    
    try:
        if not api_url:
            continue

        r = get_with_retry(api_url)
        if not r:
            continue

        soup = BeautifulSoup(r.text, "html.parser")

        # --- Safe Extraction ---
        title_tag = soup.find("h2", class_="top-card-layout__title")
        if not title_tag:
             title_tag = soup.find("h1", class_="top-card-layout__title")
        title = title_tag.get_text(strip=True) if title_tag else "N/A"

        company_tag = soup.find("a", class_="topcard__org-name-link")
        company = company_tag.get_text(strip=True) if company_tag else "N/A"

        location_tag = soup.find("span", class_="topcard__flavor--bullet")
        location = location_tag.get_text(strip=True) if location_tag else "N/A"

        desc_tag = soup.find("div", class_="show-more-less-html__markup")
        description = desc_tag.get_text("\n", strip=True) if desc_tag else "N/A"

        # --- Metadata Logic ---
        metadata = {
            "Seniority": "N/A", "Employment Type": "N/A", 
            "Job Functions": "N/A", "Industries": "N/A"
        }
        
        try:
            criteria_list = soup.find_all("li", class_="description__job-criteria-item")
            for item in criteria_list:
                label = item.find("h3")
                value = item.find("span")
                if label and value:
                    clean_label = label.get_text(strip=True).lower()
                    clean_value = value.get_text(strip=True)
                    
                    if "seniority" in clean_label or "niveau" in clean_label:
                        metadata["Seniority"] = clean_value
                    elif "employment" in clean_label or "emploi" in clean_label:
                        metadata["Employment Type"] = clean_value
                    elif "function" in clean_label or "fonction" in clean_label:
                        metadata["Job Functions"] = clean_value
                    elif "industries" in clean_label or "secteurs" in clean_label:
                        metadata["Industries"] = clean_value
        except Exception:
            pass

        # --- Exclusions ---
        if location != "N/A" and any(ex.lower() in location.lower() for ex in excluded_countries):
            continue

        # --- Regex Extraction ---
        niveau = extract_education(description)
        exp = extract_experience(description)

        data.append({
            "Date": date_str,
            "Title": title,
            "Company": company,
            "Location": location,
            "Description": description, 
            "Seniority": metadata["Seniority"],
            "Employment Type": metadata["Employment Type"],
            "Job Functions": metadata["Job Functions"],
            "Industries": metadata["Industries"],
            "API Link": api_url,
            "Original Link": original_link,
            "Niveau Étude": niveau,
            "Experience": exp,
            "Keyword": "General Search" if searched_keyword == "" else searched_keyword
        })

        time.sleep(random.uniform(0.5, 1.5))

    except Exception as e:
        print(f"\nError on item {i} ({api_url}): {e}")
        continue

print("\nScraping job done.")

# ============================
# STEP 3 — SAVE TO CSV
# ============================

if len(data) > 0:
    # Ensure data directory exists
    if not os.path.exists('data'):
        os.makedirs('data')

    df = pd.DataFrame(data)
    
    # 1. Save Full Data (with description)
    filename_full = f"data/jobs_full_{date_str}.csv"
    df.to_csv(filename_full, index=False)
    
    # 2. Save Clean Data (no description) for easier viewing
    df_clean = df.drop('Description', axis=1)
    filename_clean = f"data/jobs_clean_{date_str}.csv"
    df_clean.to_csv(filename_clean, index=False)
    
    print(f"✅ Data saved to {filename_full} and {filename_clean}")
    print(f"Total records: {len(df)}")
else:
    print("❌ No data collected.")
