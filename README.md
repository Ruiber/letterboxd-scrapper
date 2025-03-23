# Letterboxd Directors Scraper

This project performs web scraping on Letterboxd to collect statistics about film directors.  
It extracts information such as average ratings, number of viewers, and release dates for each director's films.

## 🚀 Installation

1. Clone this repository:
   ```bash
   git clone https://github.com/your-username/letterboxd-scraper.git
   cd letterboxd-scraper
    ```

2. Create a virtual environment (optional but recommended):
    ```bash
    python -m venv .venv
    source .venv/bin/activate
    ```

3. Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

## 🛠 Usage

1. Create e new file named `directors.txt` in the root directory of the project following the format:
    ```
    Director Name 1 : letterboxd-name-1
    Director Name 2 : letterboxd-name-2
    Director Name 3 : letterboxd-name-3
    ...
    ```

    Example:
    ```
    Quentin Tarantino : quentin-tarantino
    Christopher Nolan : christopher-nolan
    Martin Scorsese : martin-scorsese
    ```

2. Run the script:
    ```bash
    python main.py
    ```

3. The results will be saved in a file named `directors.csv` in the root directory of the project.

## 📌 Technologies Used

- Python 3
- `requests` for HTTP requests
- `BeautifulSoup` for HTML parsing
- `pandas` for data manipulation
- `concurrent.futures` for parallel processing
