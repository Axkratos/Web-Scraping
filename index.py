import aiohttp
import asyncio
from bs4 import BeautifulSoup
import pandas as pd

base_url = "https://merotutor.com"

# List of places and corresponding IDs
places = {
    "kathmandu": 1,
    "dhangadhi": 2,
    "biratnagar": 3,
    "pokhara": 4,
    "birgunj": 5,
    "nepalgunj": 29,
    "hetauda": 38,
    "butwal": 47,
    "narayanghat": 48,
    "mahendranagar": 49,
    "janakpur": 50
}

# List of grade categories (different URL patterns)
categories = [
    "all-subjects-teachers-grade-1-to-5",
    "all-subjects-teachers-grade-6-to-8",
    "all-subjects-teachers-grade-9-to-10",
    "all-subjects-teachers-higher-secondary",
    "all-subjects-teachers-bachelors-degree",
    "all-subjects-teachers-masters-degree"
]

# Function to decode Cloudflare's email protection
def decode_cf_email(cfemail):
    r = int(cfemail[:2], 16)  # First two hex digits are the key
    email = ''.join([chr(int(cfemail[i:i+2], 16) ^ r) for i in range(2, len(cfemail), 2)])
    return email

# Function to scrape individual user data asynchronously
async def scrape_user_data(session, user_url):
    try:
        async with session.get(user_url) as response:
            html = await response.text()
            soup = BeautifulSoup(html, 'html.parser')

            # Extract user details
            name = soup.find('div', class_='userFullName').text.strip()
            gender_age = soup.find('div', class_='userGenderAndAge userRowGeneral').text.strip()
            contact = soup.find('div', class_='userPhoneNumber userRowGeneral').text.split(":")[1].strip()

            # Extract and decode the email
            cf_email = soup.find('span', class_='__cf_email__')['data-cfemail']
            email = decode_cf_email(cf_email)

            education = soup.find('div', class_='userEducation userRowGeneral').text.split(":")[1].strip()
            experience = soup.find('div', class_='userTeachingExp userRowGeneral').text.split(":")[1].strip()
            city = soup.find_all('div', class_='userTeachingExp userRowGeneral')[1].text.split(":")[1].strip()
            teaching_location = soup.find_all('div', class_='userTeachingExp userRowGeneral')[2].text.split(":")[1].strip()
            about_me = soup.find('div', class_='userAboutMe userRowGeneral').find('span', class_='userRowValue').text.strip()

            return {
                "Name": name,
                "Gender and Age": gender_age,
                "Contact": contact,
                "Email": email,
                "Education": education,
                "Experience": experience,
                "City": city,
                "Teaching Location": teaching_location,
                "About Me": about_me
            }
    except Exception as e:
        print(f"Error scraping {user_url}: {e}")
        return None

# Function to check if a page contains profiles
def has_profiles(soup):
    return bool(soup.find('a', href=True, class_='view_profile_block'))

# Function to save data all at once to avoid delay
def save_data_to_file(user_data, output_file):
    # Convert the data to a DataFrame
    df = pd.DataFrame(user_data)
    df.to_csv(output_file, index=False)
    print(f"Saved {len(user_data)} profiles to {output_file}.")

# Function to scrape multiple places and categories asynchronously
async def scrape_category_place(session, place, place_id, category, all_user_data):
    page = 1

    while True:  # Keep scraping until no more profiles found
        if page == 1:
            paginated_url = f"{base_url}/list/{place_id}/1/{category}-{place}"
        else:
            paginated_url = f"{base_url}/list/{place_id}/1?page={page}"
        
        try:
            async with session.get(paginated_url) as response:
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')

                # If no profiles found, break out of the loop for the current category
                if not has_profiles(soup):
                    print(f"No more profiles found for {paginated_url}. Moving to next category/place.")
                    break

                # Find all profile links and preserve order (skip duplicates)
                profile_links = []
                for link in soup.find_all('a', href=True):
                    if "/users/" in link['href'] and link['href'] not in profile_links:
                        profile_links.append(link['href'])

                # Scraping profiles concurrently from each page
                tasks = [scrape_user_data(session, base_url + link) for link in profile_links]
                profiles = await asyncio.gather(*tasks)

                # Add successfully scraped profiles and ensure uniqueness
                for profile in profiles:
                    if profile and profile not in all_user_data:
                        all_user_data.append(profile)

                print(f"Scraped page {page} for {category} in {place}. Total unique profiles scraped so far: {len(all_user_data)}")
                
                # Save data every 1000 profiles
                if len(all_user_data) % 100 == 0:
                    save_data_to_file(all_user_data, 'merotutor_data.csv')
                
                page += 1  # Move to the next page
        except Exception as e:
            print(f"Error fetching page {page} for {category} in {place}: {e}")
            break
    
    return all_user_data

# Main function to orchestrate asynchronous scraping
async def main():
    all_user_data = []
    async with aiohttp.ClientSession() as session:
        tasks = []
        output_file = 'merotutor_data.csv'

        for place, place_id in places.items():
            for category in categories:
                tasks.append(scrape_category_place(session, place, place_id, category, all_user_data))

        # Gather all the results concurrently
        results = await asyncio.gather(*tasks)

        # Flatten the list of user data
        all_user_data = [item for sublist in results for item in sublist if item]

        # Save all scraped data at once if there are any remaining profiles
        if all_user_data:
            save_data_to_file(all_user_data, output_file)

    print(f"Data saved to {output_file}. Total unique profiles scraped: {len(all_user_data)}")

if __name__ == "__main__":
    asyncio.run(main())
