# LinkedIn Post Scraper Project

This project provides a set of scripts to search for, scrape, parse, and analyze LinkedIn posts focused on marketing events or companies. It offers both batch CLI tools and an interactive Streamlit UI for exploration.

---

## Project Structure

- `scraper.py`  
  Contains functions to fetch HTML content of web pages using the ScrapingBee API, supporting JavaScript rendering needed for LinkedIn pages.

- `run_pipeline.py`  
  A CLI script to run keyword-based searches for LinkedIn posts using SerpAPI and Bing, fetch posts, parse them, and save results. Outputs parsed JSON files and an aggregated Excel sheet.

- `linkedin_batch_parse_and_save.py`  
  Parses LinkedIn post HTML files saved locally from the `html_pages` folder, extracts structured data, and updates combined JSON and master Excel file with parsed results.

- `app_streamlit.py`  
  An interactive Streamlit web app enabling users to run searches, fetch, parse, filter, and visualize LinkedIn posts data. Provides live KPIs and download options.

- `parse_linkedin_post.py`  
  Centralized parsing logic extracting metadata and engagement information from raw LinkedIn HTML pages. Used by other scripts for consistency.

- `html_pages/`  
  Directory to save raw LinkedIn post HTML files for batch parsing.

- `parsed_jsons/`  
  Directory where parsed JSON results for individual posts are saved.

- `linkedin_posts_master.xlsx`  
  Excel file aggregating parsed posts data with updates from batch or pipeline runs.

- `all_posts_combined.json`  
  Combined JSON file with an array of all parsed posts.

---

## Environment Variables

- `SCRAPINGBEE_KEY`  
  API key for ScrapingBee service to fetch LinkedIn HTML content, with JS rendering.

- `SERPAPI_KEY`  
  API key for SerpAPI to perform Google custom searches for finding LinkedIn posts and companies.

- `BING_API_KEY` (optional)  
  API key for Bing search if configured for additional search capabilities (not mandatory).

Set these environment variables before running the scripts or provide API keys in the Streamlit UI.

---

## Usage

- To fetch and parse LinkedIn posts by keywords and save results, use:  
  ```bash
  python run_pipeline.py --keywords "mothers day" --top 10
  ```

- To parse previously saved HTML files in batch and generate combined results:  
  ```bash
  python linkedin_batch_parse_and_save.py
  ```

- To run the interactive Streamlit app:  
  ```bash
  streamlit run app_streamlit.py
  ```

- Use `scraper.py` module functions to fetch page HTML programmatically as needed.

---

## Notes

Ensure folders `html_pages` and `parsed_jsons` exist or scripts will create them as needed.

This project is designed to help marketing teams discover and analyze LinkedIn posts related to events, campaigns, or companies.

---

## License

MIT License
