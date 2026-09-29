import os
import sys
import time
from playwright.sync_api import sync_playwright
from dotenv import load_dotenv
from urllib.parse import quote

# Add parent directory to sys.path to find packages
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
API_KEY = os.getenv('UPSTOX_API_KEY')
RURL = os.getenv('UPSTOX_REDIRECT_URI', 'https://127.0.0.1:5000/')
rurlEncode = quote(RURL, safe='')
AUTH_URL = f'https://api-v2.upstox.com/login/authorization/dialog?response_type=code&client_id={API_KEY}&redirect_uri={rurlEncode}'

print('AUTH URL:', AUTH_URL)

with sync_playwright() as playwright:
    browser = playwright.chromium.launch(headless=True)
    page = browser.new_page()
    try:
        print('Navigating to AUTH URL...')
        page.goto(AUTH_URL, timeout=30000)
        print('Waiting for page to load...')
        page.wait_for_timeout(5000)  # Wait 5s for load/redirects
        screenshot_path = os.path.join(os.path.dirname(__file__), 'screenshot.png')
        page.screenshot(path=screenshot_path)
        print('Screenshot saved successfully to:', screenshot_path)
        print('Page Title:', page.title())
        print('Page HTML snippet:', page.content()[:2000])
    except Exception as e:
        print('Error occurred:', str(e))
    finally:
        browser.close()
