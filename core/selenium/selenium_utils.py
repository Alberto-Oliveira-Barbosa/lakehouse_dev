def get_webdriver(
    driver_type="chrome",
    arguments=None,
    experimental_options=None
):
    """
    Creates and returns a configurable WebDriver instance.

    :param driver_type: 'chrome', 'firefox', or 'edge'
    :param arguments: list of additional arguments (e.g., ["--no-sandbox"])
    :param experimental_options: dict of experimental options 
                                 (e.g., {"excludeSwitches": ["enable-automation"]})
    :return: WebDriver instance
    """

    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service as ChromeService
    from selenium.webdriver.firefox.service import Service as FirefoxService
    from selenium.webdriver.edge.service import Service as EdgeService
    from webdriver_manager.chrome import ChromeDriverManager
    from webdriver_manager.firefox import GeckoDriverManager
    from webdriver_manager.microsoft import EdgeChromiumDriverManager

    arguments = arguments or []
    experimental_options = experimental_options or {}

    driver_type = driver_type.lower()

    if driver_type == "chrome":
        options = webdriver.ChromeOptions()

        for arg in arguments:
            options.add_argument(arg)

        for key, value in experimental_options.items():
            options.add_experimental_option(key, value)

        driver = webdriver.Chrome(
            service=ChromeService(ChromeDriverManager().install()),
            options=options
        )

    elif driver_type == "firefox":
        options = webdriver.FirefoxOptions()

        for arg in arguments:
            options.add_argument(arg)

        # Firefox não possui add_experimental_option equivalente
        driver = webdriver.Firefox(
            service=FirefoxService(GeckoDriverManager().install()),
            options=options
        )

    elif driver_type == "edge":
        options = webdriver.EdgeOptions()

        for arg in arguments:
            options.add_argument(arg)

        for key, value in experimental_options.items():
            options.add_experimental_option(key, value)

        driver = webdriver.Edge(
            service=EdgeService(EdgeChromiumDriverManager().install()),
            options=options
        )

    else:
        raise ValueError("driver_type must be: 'chrome', 'firefox', or 'edge'")

    return driver

def element_exists(driver, by, value, timeout=10):
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException

    try:
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((by, value))
        )
        return True
    except TimeoutException:
        return False