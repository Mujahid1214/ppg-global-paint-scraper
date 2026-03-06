import asyncio
import logging
import csv
from datetime import datetime
from playwright.async_api import async_playwright, Page, TimeoutError as PlaywrightTimeout

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'ppg_scraper_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class PPGScraper:
    def __init__(self):
        self.region = "Europe"
        self.company = "PPG"
        self.brand = "PPG"

        # Define URLs for each category and country
        self.urls = {
            "UK": {
                "clearcoats": "https://www.ppg.com/en-GB/automotive-refinish/productlist?category=liquid_solventborne_clearcoat&productline=",
                "basecoats": "https://www.ppg.com/en-GB/automotive-refinish/productlist?category=liquid_solventborne_basecoat_toner",
                "primers": "https://www.ppg.com/en-GB/automotive-refinish/productlist?category=liquid_solventborne_undercoat"
            },
            "Italy": {
                "clearcoats": "https://www.ppg.com/it-IT/automotive-refinish/productlist?category=liquid_solventborne_clearcoat&productline=",
                "basecoats": "https://www.ppg.com/it-IT/automotive-refinish/productlist?category=liquid_waterborne_basecoat_toner",
                "primers": "https://www.ppg.com/it-IT/automotive-refinish/productlist?category=liquid_solventborne_undercoat&productline="
            }
        }

        self.products = []

    async def goto_with_retry(self, page: Page, url: str, max_retries=3):
        """Navigate to URL with retry logic"""
        for attempt in range(max_retries):
            try:
                logger.info(f"Navigating to: {url} (Attempt {attempt + 1}/{max_retries})")
                await page.goto(url, wait_until="domcontentloaded", timeout=90000)
                await page.wait_for_timeout(5000)  # Additional wait for content to load
                return True
            except Exception as e:
                logger.warning(f"Navigation attempt {attempt + 1} failed: {str(e)}")
                if attempt < max_retries - 1:
                    await page.wait_for_timeout(3000)
                else:
                    logger.error(f"Failed to navigate after {max_retries} attempts")
                    return False
        return False

    async def collect_all_product_links(self, page: Page, url: str, country: str, category: str):
        """Collect all product links from all pages using pagination"""
        try:
            logger.info(f"Collecting product links for {country} - {category}")

            # Navigate to the category page
            success = await self.goto_with_retry(page, url)
            if not success:
                return []

            all_product_urls = []
            page_number = 1

            while True:
                logger.info(f"Collecting links from page {page_number} for {country} - {category}")

                # Wait for products to load
                try:
                    await page.wait_for_selector('a[href*="/product/"]', timeout=30000)
                    await page.wait_for_timeout(2000)
                except:
                    logger.warning(f"No products found on page {page_number}")
                    break

                # Get all product links on current page
                product_links = await page.query_selector_all('a[href*="/product/"]')
                logger.info(f"Found {len(product_links)} product link elements on page {page_number}")

                # Extract href attributes from this page
                page_urls = []
                for link in product_links:
                    try:
                        href = await link.get_attribute("href")
                        if href and "/product/" in href:
                            full_url = href if href.startswith("http") else f"https://www.ppg.com{href}"
                            if full_url not in page_urls and full_url not in all_product_urls:
                                page_urls.append(full_url)
                    except:
                        continue

                logger.info(f"Collected {len(page_urls)} unique product URLs from page {page_number}")
                all_product_urls.extend(page_urls)

                # Scroll down to find pagination
                logger.info("Scrolling down to find pagination buttons")
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(3000)

                # Check for next page button
                next_button = None

                # Try different selectors for the next button
                selectors = [
                    'button:has-text("›")',
                    'a:has-text("›")',
                    'button:has-text(">")',
                    'a:has-text(">")',
                    'button[aria-label*="Next"]',
                    'a[aria-label*="Next"]'
                ]

                for selector in selectors:
                    try:
                        buttons = await page.query_selector_all(selector)
                        for btn in buttons:
                            # Check if visible
                            is_visible = await btn.is_visible()
                            if not is_visible:
                                continue

                            # Check if disabled
                            is_disabled = await btn.get_attribute("disabled")
                            aria_disabled = await btn.get_attribute("aria-disabled")

                            if not is_disabled and aria_disabled != "true":
                                next_button = btn
                                logger.info(f"Found active next button with selector: {selector}")
                                break

                        if next_button:
                            break
                    except:
                        continue

                if next_button:
                    logger.info(f"Clicking next button to move to page {page_number + 1}")
                    try:
                        await next_button.click()
                        await page.wait_for_timeout(6000)  # Wait for page to load
                        page_number += 1
                    except Exception as e:
                        logger.error(f"Error clicking next button: {str(e)}")
                        break
                else:
                    logger.info(f"No more pages found for {country} - {category}")
                    break

            logger.info(f"Total product links collected for {country} - {category}: {len(all_product_urls)}")
            return all_product_urls

        except Exception as e:
            logger.error(f"Error collecting product links for {country} - {category}: {str(e)}")
            return []

    async def scrape_product_details(self, page: Page, product_url: str, country: str, category: str):
        """Scrape details from individual product page"""
        try:
            # Navigate to product page
            success = await self.goto_with_retry(page, product_url)
            if not success:
                return None

            # Extract product name and code
            try:
                product_name_elem = await page.wait_for_selector("h1", timeout=10000)
                product_name = await product_name_elem.inner_text()
                product_name = product_name.strip()
            except:
                logger.warning(f"Product name not found on {product_url}")
                return None

            # Extract product code from product name
            product_code = product_name.split("|")[0].strip() if "|" in product_name else product_name.split()[0]

            logger.info(f"Found product: {product_name} (Code: {product_code})")

            # Scroll down to half screen to find Data Sheets section
            logger.info("Scrolling down to find Data Sheets section")
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight / 2)")
            await page.wait_for_timeout(2000)

            # Look for Data Sheets section and click it
            pdf_url = None
            try:
                # Determine the text based on country (UK: "Data Sheets", Italy: "Data Sheets" or localized)
                data_sheets_text = "Data Sheets"

                # Wait for and find Data Sheets section
                data_sheets_section = await page.wait_for_selector(f'text="{data_sheets_text}"', timeout=10000)

                if data_sheets_section:
                    logger.info("Found Data Sheets section, clicking to expand")
                    await data_sheets_section.click()
                    await page.wait_for_timeout(2000)

                    # Determine link text based on country
                    if country == "UK":
                        link_text = "Tech Data Sheet"
                    else:  # Italy
                        link_text = "Scheda tecnica"

                    # Find the Tech Data Sheet / Scheda tecnica link
                    try:
                        tech_data_link = await page.wait_for_selector(f'a:has-text("{link_text}")', timeout=5000)
                        if tech_data_link:
                            logger.info(f"Found {link_text} link, clicking to open in new tab")

                            # Listen for new page (popup/new tab)
                            async with page.context.expect_page() as new_page_info:
                                await tech_data_link.click()

                            # Get the new page
                            new_page = await new_page_info.value
                            await new_page.wait_for_load_state("networkidle", timeout=30000)

                            # Get the URL of the new page (this is the PDF URL)
                            pdf_url = new_page.url
                            logger.info(f"Successfully captured PDF URL: {pdf_url}")

                            # Close the new page
                            await new_page.close()
                        else:
                            logger.warning(f"{link_text} link not found")
                    except Exception as e:
                        logger.warning(f"Error clicking {link_text}: {str(e)}")
                else:
                    logger.warning("Data Sheets section not found")

            except Exception as e:
                logger.warning(f"Could not extract PDF URL: {str(e)}")

            product_data = {
                "Region": self.region,
                "Country": country,
                "Category": category,
                "Company": self.company,
                "Brand": self.brand,
                "Product Name": product_name,
                "Product Code": product_code,
                "PDF URL": pdf_url or "N/A"
            }

            return product_data

        except Exception as e:
            logger.error(f"Error scraping product details from {product_url}: {str(e)}")
            return None

    async def scrape_category(self, page: Page, url: str, country: str, category: str):
        """Scrape a complete category: first collect all links, then scrape each product"""
        try:
            # Step 1: Collect all product links from all pages
            logger.info(f"=== Starting {country} - {category} ===")
            product_urls = await self.collect_all_product_links(page, url, country, category)

            if not product_urls:
                logger.warning(f"No product links found for {country} - {category}")
                return

            # Step 2: Visit each product page and scrape details
            logger.info(f"Now scraping details for {len(product_urls)} products from {country} - {category}")

            for idx, product_url in enumerate(product_urls, 1):
                logger.info(f"Scraping product {idx}/{len(product_urls)}: {product_url}")

                product_data = await self.scrape_product_details(page, product_url, country, category)

                if product_data:
                    self.products.append(product_data)
                    logger.info(f"[OK] Successfully scraped: {product_data['Product Name']}")
                else:
                    logger.warning(f"[X] Failed to scrape: {product_url}")

                # Small delay between products
                await page.wait_for_timeout(100)

            logger.info(
                f"=== Completed {country} - {category}: {len([p for p in self.products if p['Country'] == country and p['Category'] == category])} products scraped ===")

        except Exception as e:
            logger.error(f"Error scraping category {country} - {category}: {str(e)}")

    async def scrape_all(self):
        """Main scraping function"""
        async with async_playwright() as p:
            logger.info("Launching browser")
            browser = await p.chromium.launch(
                headless=False,
                args=['--disable-blink-features=AutomationControlled']
            )
            context = await browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            page = await context.new_page()

            try:
                # Scrape each country and category
                for country, categories in self.urls.items():
                    for category, url in categories.items():
                        await self.scrape_category(page, url, country, category)
                        await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"Error during scraping: {str(e)}")
            finally:
                await browser.close()
                logger.info("Browser closed")

    def save_to_csv(self, filename=None, country_filter=None):
        """Save scraped products to CSV"""
        # Filter products by country if specified
        products_to_save = self.products
        if country_filter:
            products_to_save = [p for p in self.products if p['Country'] == country_filter]

        if not filename:
            country_suffix = f"_{country_filter}" if country_filter else ""
            filename = f"ppg_products{country_suffix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

        if not products_to_save:
            logger.warning(f"No products to save for {country_filter or 'all countries'}")
            return

        logger.info(f"Saving {len(products_to_save)} products to {filename}")

        fieldnames = ["Region", "Country", "Category", "Company", "Brand",
                      "Product Name", "Product Code", "PDF URL"]

        try:
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(products_to_save)

            logger.info(f"Successfully saved to {filename}")
        except Exception as e:
            logger.error(f"Error saving to CSV: {str(e)}")


async def main():
    logger.info("Starting PPG scraper")
    scraper = PPGScraper()

    try:
        await scraper.scrape_all()

        # Save to separate CSV files for UK and Italy
        logger.info("Saving data to CSV files...")
        scraper.save_to_csv(country_filter="UK")
        scraper.save_to_csv(country_filter="Italy")

        logger.info(f"Scraping completed! Total products: {len(scraper.products)}")

        # Show summary
        uk_count = len([p for p in scraper.products if p['Country'] == 'UK'])
        italy_count = len([p for p in scraper.products if p['Country'] == 'Italy'])
        logger.info(f"UK products: {uk_count}")
        logger.info(f"Italy products: {italy_count}")

    except Exception as e:
        logger.error(f"Fatal error in main: {str(e)}")


if __name__ == "__main__":
    asyncio.run(main())