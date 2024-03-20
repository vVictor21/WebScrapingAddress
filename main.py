import requests
import re
from bs4 import BeautifulSoup
import pandas as pd
from difflib import SequenceMatcher


def find_address_element(soup):
    """
    Function to find address within the soup object
    """

    address_elements = soup.select('.address, .contact, .location')
    if address_elements:
        return address_elements
    else:
        # If no elements with class="address,contact,location" are found, search for text elements matching the pattern
        text_elements = soup.find_all(string=True)
        address_elements = []
        address_pattern = re.compile(r'[A-Za-z\s]+\d+[A-Za-z\s]+[A-Z]{2}\s\d{5}|[A-Za-z]{2}\s\d{5}|[A-Za-z]\d[A-Za-z] \d[A-Za-z]\d', re.IGNORECASE)
        for text_element in text_elements:
            if address_pattern.search(text_element):
                address_elements.append(text_element)
        return address_elements

def normalize_postal_code(postal_code):
    """
    Function to normalize postal code
    """
    return re.sub(r'\s+', ' ', postal_code).strip()

def remove_number_from_road(road):
    """
    Function to remove number from road if present
    """
    if road != None:
        parts = road.split()
        if parts[0].isdigit():
            parts.pop(0)
        road_without_number = ' '.join(parts)
        return road_without_number
    else:
        return road

def parse_address(address):
    """
    Function to parse the given address into various components
    """
    country = None
    region = None
    city = None
    postcode = None
    road = None
    street_number = None

    # Use regex patterns to parse address
    country_regex = re.compile(r'(?<=, )([A-Z][A-Za-z\s]+)(?= \d{5})')
    region_regex = re.compile(r'(?<=, )([A-Z][A-Za-z\s]+)(?=, [A-Z]{2})')
    city_regex = re.compile(r'(?<=, )([A-Z][A-Za-z\s]+)(?=, \w{2} \d{5})')
    postcode_regex = re.compile(r'\b[A-Z]{2}\s\d{5}\b')
    road_regex = re.compile(r'\d+\s+[A-Z][A-Za-z\s]+')
    street_number_regex = re.compile(r'\b\d+\b')

    country_match = country_regex.search(address)
    if country_match:
        country = country_match.group(0)

    region_match = region_regex.search(address)
    if region_match:
        region = region_match.group(0)

    city_match = city_regex.search(address)
    if city_match:
        city = city_match.group(0)

    postcode_match = postcode_regex.search(address)
    if postcode_match:
        postcode = postcode_match.group(0)

    road_match = road_regex.search(address)
    if road_match:
        road = road_match.group(0)
        if road == None:
            street_number = None
        else:
            street_number_match = street_number_regex.search(address)
            if street_number_match:
                street_number = int(street_number_match.group(0))  # Convert to integer

    return {
        "country": country,
        "region": region,
        "city": city,
        "postcode": postcode,
        "road": remove_number_from_road(road),
        "street_number": street_number
    }

def extract_address_info(url):
    """
    Function to extract address information from the given url
    """
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36"
        }
        response = requests.get("http://" + url, headers=headers)

        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')

            # Find address elements in the HTML content
            address_elements = find_address_element(soup)

            if address_elements:
                addresses_by_postal = {}  # Store addresses by postal code
                for address_element in address_elements:
                    address_text = address_element.get_text().strip()
                    postal_code_match = re.search(r'\b[A-Z]{2}\s\d{5}\b', address_text)
                    if postal_code_match:
                        postal_code = normalize_postal_code(postal_code_match.group(0))
                        if postal_code not in addresses_by_postal:
                            addresses_by_postal[postal_code] = set()
                        addresses = addresses_by_postal[postal_code]
                        add_address = True
                        for addr in addresses.copy():
                            similarity = SequenceMatcher(None, address_text, addr).ratio()
                            # If addresses are similar with ratio over 0.8
                            if similarity > 0.8:
                                if len(address_text) > len(addr):
                                    addresses.remove(addr)
                                else:
                                    add_address = False
                                    break
                            elif addr in address_text:
                                addresses.remove(addr)
                        if add_address:
                            addresses.add(address_text)  # Add address to set

                for postal_code, addresses in addresses_by_postal.items():
                    max_address = max(addresses, key=len)
                    addresses_by_postal[postal_code] = {max_address}
                return addresses_by_postal

            else:
                print(f"No address elements found on {url}")
                return None
        else:
            print(f"Failed to retrieve {url}. Status code: {response.status_code}")
            return None
    except Exception as e:
        print(f"Error retrieving {url}: {e}")
        return None

def found_percentage(awfc, awnfc):
    """
    Function to calculate percentage of websites from which address info was successfully obtained.
    """
    result = awfc / (awfc + awnfc) * 100
    return f"{result:.2f}%"


def save_addresses_to_csv(addresses, filename):
    """
    Saves the found addresses to a CSV file.

    Parameters:
    - found_addresses: A list of dictionaries, where each dictionary represents an address.
    - filename: The name of the CSV file to save the data to.
    """
    # Convert the list of dictionaries into a DataFrame
    df = pd.DataFrame(addresses)

    # Save the DataFrame to a CSV file
    df.to_csv(filename, index=False)
    print(f"Data saved to {filename}.")


def main():
    """
    Main function
    """
    df = pd.read_parquet('list of company websites.snappy.parquet')
    websites = df['domain'].tolist()
    addressWebsiteFoundCounter = 0
    addressWebsiteNotFoundCounter = 0
    found_addresses = []
    addresses_not_found = []
    for domain in websites:
        print(f"Scraping {domain}...")
        addresses_by_postal = extract_address_info(domain)
        if addresses_by_postal:
            print(f"Addresses found on {domain}:")
            addressWebsiteFoundCounter += 1
            for postal_code, addresses in addresses_by_postal.items():
                for address in addresses:
                    parsed_address = parse_address(address)
                    if isinstance(parsed_address.get('street_number'), list):
                        street_number_str = ', '.join(
                            map(str, parsed_address['street_number']))
                    else:
                        street_number_str = str(parsed_address.get('street_number'))  # Access the actual street_number
                    found_addresses.append({
                        "domain": domain,
                        "postal_code": postal_code,
                        "country": parsed_address.get('country', ''),
                        "region": parsed_address.get('region', ''),
                        "city": parsed_address.get('city', ''),
                        "road": parsed_address.get('road', ''),
                        "street_number": street_number_str
                    })

                    # print info
                    print(f"Number of Websites with Address info obtained: {addressWebsiteFoundCounter}")
                    print(f"Number of Websites with no Address info found: {addressWebsiteNotFoundCounter}")
                    print(f"Total Websites Checked: {addressWebsiteFoundCounter+addressWebsiteNotFoundCounter}")
                    print(f"Percentage of founded Addresses in Websites: {found_percentage(addressWebsiteFoundCounter, addressWebsiteNotFoundCounter)}")
                    save_addresses_to_csv(found_addresses, "found_addresses.csv")

        else:
            print(f"No addresses found on {domain}")
            addressWebsiteNotFoundCounter += 1
            # print info
            print(f"Number of Websites with Address info obtained: {addressWebsiteFoundCounter}")
            print(f"Number of Websites with no Address info found: {addressWebsiteNotFoundCounter}")
            print(f"Total Websites Checked: {addressWebsiteFoundCounter + addressWebsiteNotFoundCounter}")
            print(f"Percentage of founded Addresses in Websites: {found_percentage(addressWebsiteFoundCounter, addressWebsiteNotFoundCounter)}")
            addresses_not_found.append({'domain': domain})
            save_addresses_to_csv(addresses_not_found, "addresses_not_found.csv")
        print("\n")



if __name__ == "__main__":
    main()