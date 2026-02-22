def get_news_from_site():
    """
    Scrapes the "Mais Lidas" (Most Read) news section from G1 homepage
    using Selenium WebDriver.

    This function:

    1. Initializes a Chrome WebDriver configured for Docker/Airflow environments.
    2. Accesses the G1 homepage.
    3. Waits for the dynamic content container to load.
    4. Locates the "Most Read" news block.
    5. Extracts the title and link for each news item.
    6. Stores the results in a pandas DataFrame.
    7. Ensures the WebDriver session is properly closed.

    Returns:
        None

    Side Effects:
        - Opens a browser session in headless mode.
        - Prints connection and extraction logs.
        - Creates a pandas DataFrame in memory containing:
            {
                "title": str,
                "link": str | None
            }

    Notes:
        - The site content is dynamically loaded via JavaScript,
          so explicit waits are required.
        - The Chrome arguments are configured for containerized
          environments (e.g., Docker + Airflow).
        - A fixed sleep is currently used before explicit waits,
          which could be improved by relying solely on WebDriverWait.
    """

    import logging
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from core.utils import save_on_lake, get_full_path
    
    import pandas as pd
    import time
    from core.selenium.selenium_utils import get_webdriver

    arguments = [
        "--headless=new",
        "--no-sandbox",
        "--disable-gpu",
        "--disable-dev-shm-usage",
        "--window-size=1920,1080",
        "--remote-debugging-port=9222"
    ]

    driver = get_webdriver(
        driver_type="chrome",
        arguments=arguments,
    )

    try:
        url = "https://g1.globo.com/"
        logging.info(f"Try connect to {url}")
        driver.get(url)

        time.sleep(5)

        # logging.info("Existe elemento?",
        #     len(driver.find_elements(By.CSS_SELECTOR, ".post-mais-lidas__title")))

        wait = WebDriverWait(driver, 60)

        wait.until(
            EC.presence_of_element_located((By.ID, "bstn-rtcl-placeholder"))
        )

        container = driver.find_element(By.CSS_SELECTOR, "div[data-type='post-mais-lidas']")

        elements = container.find_elements(By.CSS_SELECTOR, ".post-mais-lidas__title")

        news = []
        logging.info("Try get texts from element...")
        for elem in elements:
            title = elem.text
            try:
                link = elem.find_element(By.XPATH, "./ancestor::a").get_attribute("href")
            except:
                link = None

            if title:
                news.append({"title": title, "link": link})


        df = pd.DataFrame(news)

        full_path = get_full_path("news.csv","raw")

        logging.info(f"News:  {len(news)}")
        save_on_lake(df=df, save_path=full_path)

    finally:
        driver.quit()