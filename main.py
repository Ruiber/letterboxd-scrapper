import requests
import pandas as pd
import bs4 as BeautifulSoup
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

FILM_WORKERS = 5  # Number of simultaneous requests per director
DIRECTOR_WORKERS = 4  # Number of directors processed simultaneously

def read_directors(file_path):
    """ Reads the file and returns a dictionary of directors and their links. """
    directors = {}
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split(" : ")
            if len(parts) == 2:
                name, url_fragment = parts
                directors[name] = f"https://letterboxd.com/director/{url_fragment}/films/"
    return directors

def fetch_url(url, retries=3, timeout=10):
    """ Tries to access the URL with a limited number of attempts. """
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=timeout)
            if response.status_code == 200:
                return response.text
        except requests.RequestException as e:
            print(f"Attempt {attempt+1} failed for {url}: {e}")
        time.sleep(2)  # Wait 2 seconds before retrying
    return None

def get_film_data(url):
    """ Extracts film data from the relevant pages. """
    try:
        film_link = "https://letterboxd.com" + url
        film_stats_link = "https://letterboxd.com/csi" + url + "stats/"
        film_rating_histogram_link = "https://letterboxd.com/csi" + url + "rating-histogram/"

        film_page = fetch_url(film_link)
        if not film_page:
            return None

        soup = BeautifulSoup.BeautifulSoup(film_page, "html.parser")
        title_meta = soup.select_one('meta[property="og:title"]')
        release_year_tag = soup.select_one("div.releaseyear a")

        if not title_meta or not release_year_tag:
            return None

        film_title = title_meta["content"]
        release_year = release_year_tag.text.strip()

        # Extracting the number of people who watched
        stats_page = fetch_url(film_stats_link)
        if not stats_page:
            return None

        soup = BeautifulSoup.BeautifulSoup(stats_page, "html.parser")
        watched_by_tag = soup.select_one("li.filmstat-watches a")
        watched_by = int(re.sub(r"[^\d]", "", watched_by_tag["title"])) if watched_by_tag else 0

        # Getting the weighted average of ratings
        rating_page = fetch_url(film_rating_histogram_link)
        if not rating_page:
            return None

        soup = BeautifulSoup.BeautifulSoup(rating_page, "html.parser")
        rating_tag = soup.select_one("a.display-rating")
        weighted_average = float(re.search(r"(\d\.\d+)", rating_tag["title"]).group(1)) if rating_tag else None

        return {
            "Title": film_title,
            "Release year": release_year,
            "Watched by": watched_by,
            "Weighted average": weighted_average,
            "URL": url
        }
    except Exception as e:
        print(f"Error processing film {url}: {e}")
        return None

def get_director_films_dataframe(director_url):
    """ Returns a DataFrame containing the films of a director. """
    try:
        director_page = fetch_url(director_url)
        if not director_page:
            return None

        soup = BeautifulSoup.BeautifulSoup(director_page, "html.parser")
        film_posters = soup.select("div.film-poster")
        film_links = [div['data-target-link'] for div in film_posters]

        # Parallelizing the search for film data
        films_data = []
        with ThreadPoolExecutor(max_workers=FILM_WORKERS) as executor:  # Process up to 5 films simultaneously
            future_to_film = {executor.submit(get_film_data, link): link for link in film_links}

            for future in as_completed(future_to_film):
                film_data = future.result()
                if film_data:
                    films_data.append(film_data)

        return pd.DataFrame(films_data) if films_data else None
    except Exception as e:
        print(f"Error processing director {director_url}: {e}")
        return None

def generate_directors_statistics_csv(directors_dict, output_file):
    """ Generates statistics for each director and saves them to a CSV file. """
    director_stats = []
    total_directors = len(directors_dict)

    # Parallelizing the search for director data
    with ThreadPoolExecutor(max_workers=DIRECTOR_WORKERS) as executor:  # Process up to 4 directors simultaneously
        future_to_director = {executor.submit(get_director_films_dataframe, url): name for name, url in directors_dict.items()}

        for idx, future in enumerate(as_completed(future_to_director), 1):
            director_name = future_to_director[future]
            print(f"Processing director {idx} of {total_directors}: {director_name}")

            try:
                df_films = future.result()
                if df_films is not None and not df_films.empty:
                    stats = calculate_director_statistics(df_films)
                    if stats is not None:
                        stats["Director Name"] = director_name
                        director_stats.append(stats)
            except Exception as e:
                print(f"Error processing director {director_name}: {e}")

    stats_df = pd.DataFrame(director_stats).set_index("Director Name")
    stats_df.to_csv(output_file, index=True)
    print(f"Statistics saved to {output_file}")

def calculate_director_statistics(df):
    """ Calculates statistics based on the director's films. """
    try:
        num_films = len(df)
        latest_year = df["Release year"].max()
        earliest_year = df["Release year"].min()
        avg_rating = round(df["Weighted average"].mean(), 2)
        total_watched = df["Watched by"].sum()
        avg_watched = round(df["Watched by"].mean(), 2)
        std_watched = round(df["Watched by"].std(), 2)

        highest_rated_film = df.loc[df["Weighted average"].idxmax()]
        lowest_rated_film = df.loc[df["Weighted average"].idxmin()]

        most_watched_film = df.loc[df["Watched by"].idxmax()]
        least_watched_film = df.loc[df["Watched by"].idxmin()]

        return {
            "Number of films": num_films,
            "Latest release year": latest_year,
            "Earliest release year": earliest_year,
            "Average rating": avg_rating,
            "Total watched": total_watched,
            "Average watched": avg_watched,
            "Standard deviation of watched": std_watched,
            "Highest rated film": highest_rated_film["Title"],
            "Highest rating": highest_rated_film["Weighted average"],
            "Lowest rated film": lowest_rated_film["Title"],
            "Lowest rating": lowest_rated_film["Weighted average"],
            "Most watched film": most_watched_film["Title"],
            "Most watched": most_watched_film["Watched by"],
            "Least watched film": least_watched_film["Title"],
            "Least watched": least_watched_film["Watched by"]
        }
    except Exception as e:
        print(f"Error calculating statistics: {e}")
        return None

def main():
    directors_file_path = "directors.txt"
    output_file_path = "directors_statistics.csv"

    directors_dict = read_directors(directors_file_path)
    generate_directors_statistics_csv(directors_dict, output_file_path)

if __name__ == "__main__":
    main()
