from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)
    page = browser.new_page(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
    page.goto("https://finviz.com/groups.ashx?g=industry&v=110&o=perf1d&st=d1", timeout=30000)
    page.wait_for_timeout(3000)
    print(page.inner_text("body")[:5000])
    input("按 Enter 關閉...")
    browser.close()
