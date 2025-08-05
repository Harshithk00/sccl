import requests
import json
from datetime import datetime
import time
from typing import Dict, List, Any, Optional, Union
import re
import psycopg2
from psycopg2.extras import RealDictCursor
import psycopg2.extras
import os
from dotenv import load_dotenv

load_dotenv()

class GeMBidScraper:
    def __init__(self):
        self.session = requests.Session()
        self.base_url = "https://bidplus.gem.gov.in"
        self.all_bids_data = []
        
        # Hardcoded database configuration
        # self.db_config = {
        #     'host': 'localhost',
        #     'database': 'tender3',
        #     'user': 'postgres',
        #     'password': 'Harshit1',
        #     'port': '5432'
        # }
        
        self.db_config = {
            'host': os.getenv('DB_HOST'),
            'database': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'port': os.getenv('DB_PORT')
}
        
        # Hardcoded authentication values - UPDATE THESE WITH YOUR ACTUAL VALUES
        # self.cookie_value = "_gid=GA1.3.1407082884.1754204012; themeOption=0; _ga=GA1.3.359414268.1754204012; ci_session=b57623c09a7deb3bb6110d3a233ead39656b4da4; GeM=1474969956.20480.0000; csrf_gem_cookie=cb59431a97bd3951d10ec170d63ff1e4; TS0123c430=01e393167df10b747acf2efe5d5734596e3625f7d99cd108c61930e0b38342797ab9d62d8f4b4d3c92a50b2ec0a97af24232fec60f5bc0f333ed4e2b6cad4f212cc6fc7ce2959b908e878385d7c1b43b28034b82393fcb5eeb868520745a50b99100b084fa; _gat=1; _ga_MMQ7TYBESB=GS2.3.s1754331080$o7$g0$t1754331080$j60$l0$h0"  # Add your cookie value here
        # self.csrf_token = "cb59431a97bd3951d10ec170d63ff1e4"    # Add your CSRF token here

        self.cookie_value = os.getenv('GEM_COOKIE')
        self.csrf_token = os.getenv('CSRF_TOKEN')
        
        self.setup_database()
        
    def setup_database(self):
        """Create database table if it doesn't exist"""
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()
            
            # Create table with bid_id as primary key (non-sequential)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS bid_evaluations (
                    id VARCHAR(100) PRIMARY KEY,  -- Changed from SERIAL to VARCHAR, will store actual bid ID
                    bid_number VARCHAR(100) UNIQUE NOT NULL,
                    items TEXT,
                    quantity INTEGER,
                    ministry_name VARCHAR(500),
                    department_name VARCHAR(500),
                    start_date TIMESTAMP,
                    end_date TIMESTAMP,
                    evaluation JSONB,
                    parent_evaluation JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE INDEX IF NOT EXISTS idx_bid_number ON bid_evaluations(bid_number);
                CREATE INDEX IF NOT EXISTS idx_ministry ON bid_evaluations(ministry_name);
                CREATE INDEX IF NOT EXISTS idx_evaluation ON bid_evaluations USING GIN(evaluation);
                CREATE INDEX IF NOT EXISTS idx_parent_evaluation ON bid_evaluations USING GIN(parent_evaluation);
            """)
            
            conn.commit()
            cur.close()
            conn.close()
            print("Database setup completed successfully")
            
        except Exception as e:
            print(f"Database setup error: {e}")
    
    def fetch_all_bids_paginated(self, start_page: int = 1, end_page: int = 1000):
        """
        Fetch all bids data from the API with pagination
        """
        if not self.cookie_value or not self.csrf_token:
            print("ERROR: cookie_value and csrf_token must be set!")
            print("Please update the hardcoded values in the __init__ method")
            return []
        
        url = f"{self.base_url}/all-bids-data"
        
        headers = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Cookie": self.cookie_value,
            "Origin": "https://bidplus.gem.gov.in",
            "Referer": "https://bidplus.gem.gov.in/all-bids",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36 Edg/138.0.0.0",
            "X-Requested-With": "XMLHttpRequest"
        }
        
        all_docs = []
        
        for page in range(start_page, end_page + 1):
            print(f"Fetching page {page}/{end_page}...")
            
            payload = {
                "payload": json.dumps({
                    "page": page,
                    "param": {
                        "searchBid": "",
                        "searchType": "fullText"
                    },
                    "filter": {
                        "bidStatusType": "bidrastatus",
                        "byType": "all",
                        "highBidValue": "",
                        "byEndDate": {
                            "from": "2025-01-01",
                            "to": "2025-03-01"
                        },
                        "sort": "Bid-End-Date-Latest",
                        "byStatus": "bid_awarded"
                    }
                }),
                "csrf_bd_gem_nk": self.csrf_token
            }
            
            try:
                response = self.session.post(url, headers=headers, data=payload)
                response.raise_for_status()
                
                data = response.json()
                
                # Handle the actual response structure
                if data.get('status') == 1 and data.get('response', {}).get('response', {}).get('docs'):
                    docs = data['response']['response']['docs']
                    num_found = data['response']['response']['numFound']
                    
                    print(f"Page {page}: Found {len(docs)} bids (Total in system: {num_found})")
                    
                    if len(docs) == 0:
                        print(f"No more bids found on page {page}. Stopping pagination.")
                        break
                    
                    all_docs.extend(docs)
                    
                    # Add delay between requests to avoid overwhelming the server
                    time.sleep(0.5)
                    
                else:
                    print(f"Page {page}: No bids found or unexpected response structure")
                    # Continue to next page instead of breaking, might be temporary issue
                    
            except requests.exceptions.RequestException as e:
                print(f"Error fetching page {page}: {e}")
                # Continue to next page instead of breaking
                time.sleep(2)  # Longer delay on error
                continue
        
        print(f"Total bids fetched across all pages: {len(all_docs)}")
        return all_docs
    
    def extract_bid_info(self, bid: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract required information from a bid based on actual API response structure
        """
        # Helper function to extract first element from list or return the value directly
        def get_value(field: Any) -> Any:
            if isinstance(field, list) and len(field) > 0:
                return field[0]
            return field
        
        bid_info = {
            "id": get_value(bid.get("id")),
            "b_bid_number": get_value(bid.get("b_bid_number")),
            "b_category_name": get_value(bid.get("b_category_name")),
            "b_total_quantity": get_value(bid.get("b_total_quantity")),
            "b_status": get_value(bid.get("b_status")),
            "final_start_date_sort": get_value(bid.get("final_start_date_sort")),
            "final_end_date_sort": get_value(bid.get("final_end_date_sort")),
            "ba_official_details_minName": get_value(bid.get("ba_official_details_minName")),
            "ba_official_details_deptName": get_value(bid.get("ba_official_details_deptName"))
        }
        
        # Check if parent bid exists - look for b_id_parent or b_bid_number_parent
        b_id_parent = get_value(bid.get("b_id_parent"))
        b_bid_number_parent = get_value(bid.get("b_bid_number_parent"))
        
        if b_id_parent or b_bid_number_parent:
            bid_info.update({
                "b_bid_number_parent": b_bid_number_parent,
                "b_id_parent": b_id_parent,
                "b_cat_id": get_value(bid.get("b_cat_id"))
            })
        
        # Add additional fields that might be useful
        bid_info["b_cat_id"] = get_value(bid.get("b_cat_id"))
        bid_info["b_eval_type"] = get_value(bid.get("b_eval_type"))
        bid_info["bbt_title"] = get_value(bid.get("bbt_title"))
        
        return bid_info
    
    def get_bid_result_view(self, bid_id: str, is_parent: bool = False) -> Optional[Dict[str, Any]]:
        """
        Get detailed bid result view with enhanced evaluation extraction
        """
        if is_parent:
            url = f"{self.base_url}/bidding/bid/getSinglePacketResultView/{bid_id}"
        else:
            url = f"{self.base_url}/bidding/bid/getBidResultView/{bid_id}"
        
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
            "Cookie": self.cookie_value,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36 Edg/138.0.0.0",
            "Referer": f"https://bidplus.gem.gov.in/bidding/bid/show/{bid_id}"
        }
        
        try:
            response = self.session.get(url, headers=headers)
            response.raise_for_status()
            
            # Parse the HTML response to extract evaluation data
            html_content = response.text
            
            evaluation_data: Dict[str, Any] = {
                "has_financial_evaluation": False,
                "has_technical_evaluation": False,
                "has_general_evaluation": False,
                "sellers_participated": []
            }
            
            # Extract sellers participation data using BeautifulSoup if available
            try:
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(html_content, 'html.parser')
                
                # Enhanced extraction method - look for all evaluation sections
                evaluation_data = self.extract_all_evaluations(soup, evaluation_data)
                
                # Also try to extract parent bid ID from the HTML if present
                parent_bid_id = self.extract_parent_bid_id_from_html(html_content)
                if parent_bid_id:
                    evaluation_data["parent_bid_id_found"] = parent_bid_id
                
            except ImportError:
                print("  BeautifulSoup not available - using basic string matching")
                # Enhanced fallback method using regex
                evaluation_data = self.extract_evaluations_with_regex(html_content, evaluation_data)
                
            except Exception as e:
                print(f"  Error parsing HTML for bid {bid_id}: {e}")
                # Try regex fallback
                evaluation_data = self.extract_evaluations_with_regex(html_content, evaluation_data)
            
            return evaluation_data
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching bid result view for ID {bid_id}: {e}")
            return None
    
    def extract_parent_bid_id_from_html(self, html_content: str) -> Optional[str]:
        """Extract parent bid ID from HTML content"""
        # Look for parent bid patterns in the HTML
        parent_patterns = [
            r'Parent\s*Bid\s*ID[:\s]*(\d+)',
            r'parent[_\s]*bid[_\s]*id[:\s]*(\d+)',
            r'getSinglePacketResultView/(\d+)',
            r'b_id_parent[:\s]*(\d+)'
        ]
        
        for pattern in parent_patterns:
            match = re.search(pattern, html_content, re.IGNORECASE)
            if match:
                return match.group(1)
        
        return None
    
    def extract_all_evaluations(self, soup, evaluation_data: Dict[str, Any]) -> Dict[str, Any]:
        """Enhanced evaluation extraction using BeautifulSoup"""
        
        # Track all sellers across all evaluation types
        all_sellers = []
        
        # Find all panel headings to identify evaluation types
        panel_headings = soup.find_all('div', class_='panel-heading')
        
        for heading in panel_headings:
            heading_text = heading.get_text().strip()
            print(f"    Found panel heading: {heading_text}")
            
            # Check for different evaluation types with more patterns
            if any(term in heading_text.upper() for term in ['TECHNICAL', 'TECH EVAL', 'TECHNICAL EVALUATION']):
                evaluation_data["has_technical_evaluation"] = True
                technical_sellers = self.extract_technical_evaluation(soup, heading)
                all_sellers.extend(technical_sellers)
                print(f"    Extracted {len(technical_sellers)} technical evaluations")
                
            elif any(term in heading_text.upper() for term in ['FINANCIAL', 'FIN EVAL', 'FINANCIAL EVALUATION']):
                evaluation_data["has_financial_evaluation"] = True
                financial_sellers = self.extract_financial_evaluation(soup, heading)
                all_sellers.extend(financial_sellers)
                print(f"    Extracted {len(financial_sellers)} financial evaluations")
                
            elif 'EVALUATION' in heading_text.upper() and not any(term in heading_text.upper() for term in ['TECHNICAL', 'FINANCIAL']):
                # This is the general "Evaluation" case
                evaluation_data["has_general_evaluation"] = True
                general_sellers = self.extract_general_evaluation(soup, heading)
                all_sellers.extend(general_sellers)
                print(f"    Extracted {len(general_sellers)} general evaluations")
        
        # Also look for tables directly without panel headings
        if not evaluation_data["has_general_evaluation"] and not evaluation_data["has_financial_evaluation"]:
            # Look for any evaluation tables
            tables = soup.find_all('table', class_='table')
            for table in tables:
                if self.is_evaluation_table(table):
                    sellers = self.extract_sellers_from_table(table)
                    if sellers:
                        all_sellers.extend(sellers)
                        evaluation_data["has_general_evaluation"] = True
                        print(f"    Found evaluation table with {len(sellers)} sellers")
        
        # Store only the combined sellers list
        evaluation_data["sellers_participated"] = all_sellers
        
        return evaluation_data
    
    def is_evaluation_table(self, table) -> bool:
        """Check if a table contains evaluation data"""
        if not table:
            return False
        
        # Check header row for evaluation indicators
        header_row = table.find('tr')
        if header_row:
            header_text = header_row.get_text().upper()
            return any(term in header_text for term in [
                'SELLER', 'VENDOR', 'BIDDER', 'RANK', 'PRICE', 'STATUS', 'QUALIFIED'
            ])
        return False
    
    def extract_sellers_from_table(self, table) -> List[Dict[str, Any]]:
        """Extract seller information from any evaluation table"""
        sellers = []
        if not table:
            return sellers
        
        rows = table.find_all('tr')
        if len(rows) < 2:  # Need header + at least one data row
            return sellers
        
        # Analyze header to determine column structure
        header_row = rows[0]
        headers = [th.get_text().strip().upper() for th in header_row.find_all(['th', 'td'])]
        
        # Find column indices
        seller_col = next((i for i, h in enumerate(headers) if 'SELLER' in h or 'VENDOR' in h or 'NAME' in h), 1)
        price_col = next((i for i, h in enumerate(headers) if 'PRICE' in h or 'AMOUNT' in h), -1)
        rank_col = next((i for i, h in enumerate(headers) if 'RANK' in h), -1)
        status_col = next((i for i, h in enumerate(headers) if 'STATUS' in h), -1)
        
        # Extract data rows
        for row in rows[1:]:
            cells = row.find_all('td')
            if len(cells) > seller_col:
                # Extract seller name
                seller_name_cell = cells[seller_col]
                seller_name = self.clean_seller_name(seller_name_cell.get_text())
                
                seller_info = {
                    "s_no": cells[0].get_text(strip=True) if len(cells) > 0 else "",
                    "seller_name": seller_name,
                    "offered_item": cells[2].get_text(strip=True) if len(cells) > 2 else "",
                }
                
                # Add price if available
                if price_col >= 0 and len(cells) > price_col:
                    price_cell = cells[price_col]
                    price_span = price_cell.find('span', class_='bid_price')
                    price = price_span.get_text(strip=True) if price_span else price_cell.get_text(strip=True)
                    seller_info["total_price"] = price
                
                # Add rank if available
                if rank_col >= 0 and len(cells) > rank_col:
                    rank_cell = cells[rank_col]
                    rank_strong = rank_cell.find('strong')
                    rank = rank_strong.get_text(strip=True) if rank_strong else rank_cell.get_text(strip=True)
                    seller_info["rank"] = rank
                
                # Add status if available
                if status_col >= 0 and len(cells) > status_col:
                    status_cell = cells[status_col]
                    status_span = status_cell.find('span')
                    status = status_span.get_text(strip=True) if status_span else status_cell.get_text(strip=True)
                    seller_info["status"] = status
                
                sellers.append(seller_info)
        
        return sellers
    
    def clean_seller_name(self, raw_name: str) -> str:
        """Clean up seller name by removing MSE tags and extra formatting"""
        if not raw_name:
            return ""
        
        # Remove HTML tags
        clean_name = re.sub(r'<[^>]+>', '', raw_name)
        
        # Remove MSE category info in parentheses
        clean_name = re.sub(r'\s*\([^)]*MSE[^)]*\)', '', clean_name, flags=re.IGNORECASE)
        clean_name = re.sub(r'\s*\([^)]*Social Category[^)]*\)', '', clean_name, flags=re.IGNORECASE)
        
        # Remove extra whitespace and line breaks
        clean_name = re.sub(r'\s+', ' ', clean_name).strip()
        
        # Take only the first line if multiple lines
        clean_name = clean_name.split('\n')[0].strip()
        
        return clean_name
    
    def extract_evaluations_with_regex(self, html_content: str, evaluation_data: Dict[str, Any]) -> Dict[str, Any]:
        """Fallback method using regex when BeautifulSoup is not available"""
        
        # Look for evaluation indicators
        if re.search(r'technical\s+evaluat', html_content, re.IGNORECASE):
            evaluation_data["has_technical_evaluation"] = True
        
        if re.search(r'financial\s+evaluat', html_content, re.IGNORECASE):
            evaluation_data["has_financial_evaluation"] = True
        
        if re.search(r'evaluation|sellers?\s+participated', html_content, re.IGNORECASE):
            evaluation_data["has_general_evaluation"] = True
        
        # Try to extract seller names using regex
        seller_patterns = [
            r'<td[^>]*>\s*<span[^>]*>\s*([A-Z][A-Z\s&/.,-]+?)\s*<br',  # Seller names before <br>
            r'<td[^>]*>\s*([A-Z][A-Z\s&/.,-]{10,}?)\s*</td>',  # Direct seller names in TD
        ]
        
        sellers_found = []
        for pattern in seller_patterns:
            matches = re.findall(pattern, html_content, re.IGNORECASE)
            for match in matches[:10]:  # Limit to first 10 matches
                clean_name = self.clean_seller_name(match)
                if len(clean_name) > 5:  # Only include substantial names
                    sellers_found.append({
                        "seller_name": clean_name,
                        "extraction_method": "regex"
                    })
        
        if sellers_found:
            evaluation_data["sellers_participated"] = sellers_found
            print(f"    Regex extraction found {len(sellers_found)} sellers")
        
        return evaluation_data
    
    def extract_technical_evaluation(self, soup, heading) -> List[Dict[str, Any]]:
        """Extract technical evaluation data with enhanced parsing"""
        technical_sellers = []
        
        # Find the panel body associated with this heading
        panel = heading.find_parent('div', class_='panel')
        if panel:
            technical_sections = panel.find_all('div', class_='technical_eligible')
            
            for section in technical_sections:
                table = section.find('table')
                if table and hasattr(table, 'find_all'):
                    rows = table.find_all('tr')
                    
                    # Check if this is technical evaluation
                    header_row = rows[0] if rows else None
                    if header_row:
                        header_text = header_row.get_text()
                        
                        # Process technical evaluation rows
                        for row in rows[1:] if len(rows) > 1 else []:
                            if hasattr(row, 'find_all'):
                                cells = row.find_all('td')
                                if len(cells) >= 6:  # Minimum columns for technical evaluation
                                    
                                    # Extract S.No
                                    s_no = cells[0].get_text(strip=True)
                                    
                                    # Extract seller name from span with class 'cid'
                                    seller_name_cell = cells[1]
                                    seller_name_span = seller_name_cell.find('span', class_='cid')
                                    raw_name = seller_name_span.get_text(strip=True) if seller_name_span else seller_name_cell.get_text(strip=True)
                                    seller_name = self.clean_seller_name(raw_name)
                                    
                                    # Extract offered item (usually column 2)
                                    offered_item = cells[2].get_text(strip=True)
                                    
                                    # Extract participated date (usually column 3)
                                    participated_on = cells[3].get_text(strip=True)
                                    
                                    # Extract EMD Status (usually column 4)
                                    emd_status_cell = cells[4]
                                    emd_status_span = emd_status_cell.find('span')
                                    emd_status = emd_status_span.get_text(strip=True) if emd_status_span else emd_status_cell.get_text(strip=True)
                                    
                                    # Extract MSE/MII Status (usually column 5, if exists)
                                    mse_status = ""
                                    if len(cells) > 5:
                                        mse_status_cell = cells[5]
                                        # Extract all MSE/MII labels
                                        mse_labels = mse_status_cell.find_all('span', class_='label')
                                        mse_status_list = [label.get_text(strip=True) for label in mse_labels]
                                        mse_status = ", ".join(mse_status_list)
                                    
                                    # Extract status (Qualified/Disqualified) - usually last column
                                    status_cell = cells[-1]  # Last column
                                    status_span = status_cell.find('span')
                                    if status_span:
                                        status_text = status_span.get_text(strip=True)
                                        # Normalize status text
                                        if 'Qualified' in status_text:
                                            status = "Qualified"
                                        elif 'Disqualified' in status_text:
                                            status = "Disqualified"
                                        else:
                                            status = status_text
                                    else:
                                        status = status_cell.get_text(strip=True)
                                    
                                    technical_info = {
                                        "s_no": s_no,
                                        "seller_name": seller_name,
                                        "offered_item": offered_item,
                                        "participated_on": participated_on,
                                        "emd_status": emd_status,
                                        "mse_status": mse_status,
                                        "status": status,
                                        "evaluation_type": "technical"
                                    }
                                    technical_sellers.append(technical_info)
        
        return technical_sellers
    
    def extract_financial_evaluation(self, soup, heading) -> List[Dict[str, Any]]:
        """Extract financial evaluation data"""
        financial_sellers = []
        
        # Find the panel body associated with this heading
        panel = heading.find_parent('div', class_='panel')
        if panel:
            # Look for financial evaluation indicators
            financial_indicators = panel.find_all('label', string=lambda text: text and any(
                phrase in text for phrase in ['List of Sellers Qualified Financially', 'Financial Evaluation', 'Price Comparison']
            ))
            
            for financial_label in financial_indicators:
                # Find the table after the financial label
                table = financial_label.find_next('table')
                if table and hasattr(table, 'find_all'):
                    rows = table.find_all('tr')
                    
                    # Skip header row and process data rows
                    for row in rows[1:] if len(rows) > 1 else []:
                        if hasattr(row, 'find_all'):
                            cells = row.find_all('td')
                            if len(cells) >= 4:
                                # Extract seller name (remove MSE tags and clean up)
                                seller_name_cell = cells[1] if len(cells) > 1 else cells[0]
                                raw_name = seller_name_cell.get_text()
                                seller_name = self.clean_seller_name(raw_name)
                                
                                # Extract price
                                price_cell = cells[3] if len(cells) > 3 else None
                                price = ""
                                if price_cell:
                                    price_span = price_cell.find('span', class_='bid_price')
                                    price = price_span.get_text(strip=True) if price_span else price_cell.get_text(strip=True)
                                
                                # Extract rank
                                rank_cell = cells[4] if len(cells) > 4 else None
                                rank = ""
                                if rank_cell:
                                    rank_strong = rank_cell.find('strong')
                                    rank = rank_strong.get_text(strip=True) if rank_strong else rank_cell.get_text(strip=True)
                                
                                financial_info = {
                                    "s_no": cells[0].get_text(strip=True),
                                    "seller_name": seller_name,
                                    "offered_item": cells[2].get_text(strip=True) if len(cells) > 2 else "",
                                    "total_price": price,
                                    "rank": rank,
                                    "evaluation_type": "financial"
                                }
                                financial_sellers.append(financial_info)
        
        return financial_sellers
    
    def extract_general_evaluation(self, soup, heading) -> List[Dict[str, Any]]:
        """Extract general evaluation data (just 'Evaluation')"""
        general_sellers = []
        
        # Find the panel body associated with this heading
        panel = heading.find_parent('div', class_='panel')
        if panel:
            technical_sections = panel.find_all('div', class_='technical_eligible')
            
            for section in technical_sections:
                table = section.find('table')
                if table and hasattr(table, 'find_all'):
                    sellers = self.extract_sellers_from_table(table)
                    # Add evaluation type to each seller
                    for seller in sellers:
                        seller["evaluation_type"] = "general"
                    general_sellers.extend(sellers)
        
        return general_sellers
    
    def parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse date string to datetime object"""
        if not date_str:
            return None
        
        try:
            # Handle ISO format dates
            if 'T' in date_str and 'Z' in date_str:
                return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            
            # Add other date format parsing as needed
            return datetime.fromisoformat(date_str)
        except:
            return None
    
    def prepare_evaluation_for_database(self, evaluation_data: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare minimal evaluation data for database storage"""
        if not evaluation_data:
            return {}
        
        # Only include essential fields for database
        minimal_evaluation = {
            "has_financial_evaluation": evaluation_data.get("has_financial_evaluation", False),
            "has_technical_evaluation": evaluation_data.get("has_technical_evaluation", False),
            "has_general_evaluation": evaluation_data.get("has_general_evaluation", False),
            "sellers_participated": evaluation_data.get("sellers_participated", [])
        }
        
        # Include parent_bid_id_found if it exists
        if "parent_bid_id_found" in evaluation_data:
            minimal_evaluation["parent_bid_id_found"] = evaluation_data["parent_bid_id_found"]
        
        return minimal_evaluation
    
    def save_to_database(self, bid_info: Dict[str, Any]) -> bool:
        """Save bid information to PostgreSQL database"""
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()
            
            # Prepare data for insertion
            bid_id = bid_info.get('id', '')  # This will be the actual bid ID from the API
            bid_number = bid_info.get('b_bid_number', '')
            items = bid_info.get('b_category_name', '')
            quantity = bid_info.get('b_total_quantity', 0)
            ministry_name = bid_info.get('ba_official_details_minName', '')
            department_name = bid_info.get('ba_official_details_deptName', '')
            start_date = self.parse_date(bid_info.get('final_start_date_sort', ''))
            end_date = self.parse_date(bid_info.get('final_end_date_sort', ''))
            
            # Prepare minimal evaluation JSON - only essential fields
            evaluation_data = bid_info.get('evaluation_data', {})
            parent_evaluation_data = bid_info.get('parent_evaluation_data', {})
            
            # Use the new method to prepare minimal data
            evaluation_json = json.dumps(self.prepare_evaluation_for_database(evaluation_data)) if evaluation_data else None
            parent_evaluation_json = json.dumps(self.prepare_evaluation_for_database(parent_evaluation_data)) if parent_evaluation_data else None
            
            # Insert or update record using bid_id as the primary key
            cur.execute("""
                INSERT INTO bid_evaluations 
                (id, bid_number, items, quantity, ministry_name, department_name, start_date, end_date, evaluation, parent_evaluation)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) 
                DO UPDATE SET
                    bid_number = EXCLUDED.bid_number,
                    items = EXCLUDED.items,
                    quantity = EXCLUDED.quantity,
                    ministry_name = EXCLUDED.ministry_name,
                    department_name = EXCLUDED.department_name,
                    start_date = EXCLUDED.start_date,
                    end_date = EXCLUDED.end_date,
                    evaluation = EXCLUDED.evaluation,
                    parent_evaluation = EXCLUDED.parent_evaluation,
                    updated_at = CURRENT_TIMESTAMP
            """, (bid_id, bid_number, items, quantity, ministry_name, department_name, 
                  start_date, end_date, evaluation_json, parent_evaluation_json))
            
            conn.commit()
            cur.close()
            conn.close()
            
            print(f"  ✓ Saved to database: {bid_number} (ID: {bid_id})")
            return True
            
        except Exception as e:
            print(f"  ✗ Database error for {bid_info.get('b_bid_number', 'Unknown')} (ID: {bid_info.get('id', 'Unknown')}): {e}")
            return False
    
    def process_all_bids(self, start_page: int = 1, end_page: int = 1000):
        """
        Main processing function with enhanced evaluation extraction and database storage
        """
        print("=== Enhanced GeM Bid Data Scraper with PostgreSQL Storage (Minimal JSON) ===")
        print(f"Processing pages {start_page} to {end_page}")
        
        # Check if authentication values are set
        if not self.cookie_value or not self.csrf_token:
            print("\n❌ ERROR: Authentication values not set!")
            print("Please update the following in the __init__ method:")
            print("- self.cookie_value = 'your_cookie_value_here'")
            print("- self.csrf_token = 'your_csrf_token_here'")
            print("\nTo get these values:")
            print("1. Visit https://bidplus.gem.gov.in/all-bids in your browser")
            print("2. Open Developer Tools -> Network tab")
            print("3. Make a search request and copy the Cookie header and csrf_bd_gem_nk values")
            return []
        
        # Fetch all bids with pagination
        print("\nFetching all bids data with pagination...")
        bids_data = self.fetch_all_bids_paginated(start_page, end_page)
        
        if not bids_data:
            print("No bids data retrieved. Exiting.")
            return []
        
        processed_bids = []
        successful_saves = 0
        failed_saves = 0
        
        for idx, bid in enumerate(bids_data):
            print(f"\nProcessing bid {idx + 1}/{len(bids_data)}: {bid.get('b_bid_number', 'Unknown')}")
            
            # Extract basic bid info
            bid_info = self.extract_bid_info(bid)
            
            # Get detailed bid result view
            if bid_info["id"]:
                print(f"  Fetching result view for bid ID: {bid_info['id']}")
                result_view = self.get_bid_result_view(bid_info["id"])
                if result_view:
                    bid_info["evaluation_data"] = result_view
                    
                    # Check if main bid has no evaluation data (all False)
                    has_any_evaluation = (result_view.get("has_financial_evaluation", False) or 
                                        result_view.get("has_technical_evaluation", False) or 
                                        result_view.get("has_general_evaluation", False))
                    
                    if not has_any_evaluation:
                        print(f"  No evaluation found in main view, trying getSinglePacketResultView...")
                        single_packet_view = self.get_bid_result_view(bid_info["id"], is_parent=True)
                        if single_packet_view:
                            # Check if single packet view has evaluation data
                            has_single_packet_evaluation = (single_packet_view.get("has_financial_evaluation", False) or 
                                                           single_packet_view.get("has_technical_evaluation", False) or 
                                                           single_packet_view.get("has_general_evaluation", False))
                            
                            if has_single_packet_evaluation:
                                print(f"  Found evaluation data in getSinglePacketResultView!")
                                bid_info["evaluation_data"] = single_packet_view
                                bid_info["evaluation_source"] = "single_packet_view"
                            else:
                                bid_info["evaluation_source"] = "main_view_empty"
                        else:
                            bid_info["evaluation_source"] = "main_view_empty"
                    else:
                        bid_info["evaluation_source"] = "main_view"
                
                # If parent bid exists, get its result view too
                if bid_info.get("b_id_parent"):
                    print(f"  Fetching parent result view for bid ID: {bid_info['b_id_parent']}")
                    parent_result_view = self.get_bid_result_view(str(bid_info["b_id_parent"]), is_parent=True)
                    if parent_result_view:
                        bid_info["parent_evaluation_data"] = parent_result_view
                
                # Check if evaluation data contains parent bid reference
                elif result_view and result_view.get("parent_bid_id_found"):
                    parent_id = result_view["parent_bid_id_found"]
                    print(f"  Found parent bid ID in HTML: {parent_id}")
                    parent_result_view = self.get_bid_result_view(parent_id, is_parent=True)
                    if parent_result_view:
                        bid_info["parent_evaluation_data"] = parent_result_view
                        bid_info["b_id_parent"] = parent_id
            
            processed_bids.append(bid_info)
            
            # Enhanced display of current bid info
            self.display_bid_info(bid_info)
            
            # Save to database
            if self.save_to_database(bid_info):
                successful_saves += 1
            else:
                failed_saves += 1
            
            print("  " + "-" * 80)
            
            # Add delay to avoid overwhelming the server
            time.sleep(1)
        
        print(f"\n=== Processing Complete ===")
        print(f"Total bids processed: {len(processed_bids)}")
        print(f"Successfully saved to database: {successful_saves}")
        print(f"Failed to save: {failed_saves}")
        
        # Summary statistics
        technical_count = sum(1 for bid in processed_bids if bid.get("evaluation_data", {}).get("has_technical_evaluation"))
        financial_count = sum(1 for bid in processed_bids if bid.get("evaluation_data", {}).get("has_financial_evaluation"))
        general_count = sum(1 for bid in processed_bids if bid.get("evaluation_data", {}).get("has_general_evaluation"))
        parent_count = sum(1 for bid in processed_bids if bid.get("parent_evaluation_data"))
        
        print(f"\nEvaluation Statistics:")
        print(f"  Technical Evaluations: {technical_count}")
        print(f"  Financial Evaluations: {financial_count}")
        print(f"  General Evaluations: {general_count}")
        print(f"  Parent Bid Evaluations: {parent_count}")
        
        return processed_bids
    
    def display_bid_info(self, bid_info: Dict[str, Any]):
        """Enhanced display of bid information"""
        print(f"  Bid ID: {bid_info['id']}")  # Now showing the actual bid ID
        print(f"  Bid Number: {bid_info['b_bid_number']}")
        print(f"  Category: {bid_info['b_category_name']}")
        print(f"  Quantity: {bid_info['b_total_quantity']}")
        print(f"  Status: {bid_info['b_status']}")
        print(f"  Start Date: {bid_info['final_start_date_sort']}")
        print(f"  End Date: {bid_info['final_end_date_sort']}")
        print(f"  Ministry: {bid_info['ba_official_details_minName']}")
        print(f"  Department: {bid_info['ba_official_details_deptName']}")
        print(f"  Category ID: {bid_info['b_cat_id']}")
        if bid_info.get("bbt_title"):
            print(f"  Title: {bid_info['bbt_title']}")
        if bid_info.get("b_id_parent"):
            print(f"  Parent Bid ID: {bid_info['b_id_parent']}")
            print(f"  Parent Bid Number: {bid_info['b_bid_number_parent']}")
        
        # Show evaluation source
        if bid_info.get("evaluation_source"):
            print(f"  Evaluation Source: {bid_info['evaluation_source']}")
        
        # Display evaluation data if available
        evaluation_data = bid_info.get("evaluation_data")
        if evaluation_data and isinstance(evaluation_data, dict):
            print(f"  MAIN BID EVALUATION:")
            print(f"    Financial: {'Yes' if evaluation_data.get('has_financial_evaluation', False) else 'No'}")
            print(f"    Technical: {'Yes' if evaluation_data.get('has_technical_evaluation', False) else 'No'}")
            print(f"    General: {'Yes' if evaluation_data.get('has_general_evaluation', False) else 'No'}")
            
            # Display sellers participated summary
            sellers = evaluation_data.get('sellers_participated', [])
            if isinstance(sellers, list) and sellers:
                print(f"    Total Sellers Participated: {len(sellers)}")
                # Show first 3 sellers
                for i, seller in enumerate(sellers[:3]):
                    if isinstance(seller, dict):
                        print(f"      {seller.get('s_no', i+1)}. {seller.get('seller_name', 'Unknown')}")
                        if 'total_price' in seller and seller['total_price']:
                            print(f"         Price: ₹{seller.get('total_price')}")
                        if 'rank' in seller and seller['rank']:
                            print(f"         Rank: {seller.get('rank')}")
                        if 'status' in seller and seller['status']:
                            print(f"         Status: {seller.get('status')}")
                        if 'evaluation_type' in seller:
                            print(f"         Type: {seller.get('evaluation_type')}")
                
                if len(sellers) > 3:
                    print(f"      ... and {len(sellers) - 3} more participants")
        
        # Display parent evaluation data if available
        parent_evaluation_data = bid_info.get("parent_evaluation_data")
        if parent_evaluation_data and isinstance(parent_evaluation_data, dict):
            print(f"  PARENT BID EVALUATION:")
            print(f"    Financial: {'Yes' if parent_evaluation_data.get('has_financial_evaluation', False) else 'No'}")
            print(f"    Technical: {'Yes' if parent_evaluation_data.get('has_technical_evaluation', False) else 'No'}")
            print(f"    General: {'Yes' if parent_evaluation_data.get('has_general_evaluation', False) else 'No'}")
            
            # Show summary of parent evaluation
            parent_sellers = parent_evaluation_data.get('sellers_participated', [])
            if isinstance(parent_sellers, list) and parent_sellers:
                print(f"    Parent Total Sellers: {len(parent_sellers)}")
                # Show top 2 parent sellers
                for i, seller in enumerate(parent_sellers[:2]):
                    if isinstance(seller, dict):
                        print(f"      {seller.get('s_no', i+1)}. {seller.get('seller_name', 'Unknown')}")
                        if 'total_price' in seller and seller['total_price']:
                            print(f"         Price: ₹{seller.get('total_price')}")
                        if 'rank' in seller and seller['rank']:
                            print(f"         Rank: {seller.get('rank')}")

    def get_database_stats(self):
        """Get statistics from the database"""
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()
            
            # Total records
            cur.execute("SELECT COUNT(*) FROM bid_evaluations")
            total_records = cur.fetchone()[0]
            
            # Records with evaluation data
            cur.execute("SELECT COUNT(*) FROM bid_evaluations WHERE evaluation IS NOT NULL")
            with_evaluation = cur.fetchone()[0]
            
            # Records with parent evaluation data
            cur.execute("SELECT COUNT(*) FROM bid_evaluations WHERE parent_evaluation IS NOT NULL")
            with_parent_evaluation = cur.fetchone()[0]
            
            # Records with sellers participated
            cur.execute("""
                SELECT COUNT(*) FROM bid_evaluations 
                WHERE evaluation->>'sellers_participated' != '[]' 
                AND evaluation->>'sellers_participated' IS NOT NULL
            """)
            with_sellers = cur.fetchone()[0]
            
            # Top ministries
            cur.execute("""
                SELECT ministry_name, COUNT(*) as count 
                FROM bid_evaluations 
                WHERE ministry_name IS NOT NULL 
                GROUP BY ministry_name 
                ORDER BY count DESC 
                LIMIT 5
            """)
            top_ministries = cur.fetchall()
            
            # Show some sample evaluation data to verify minimal storage
            cur.execute("""
                SELECT id, bid_number, evaluation 
                FROM bid_evaluations 
                WHERE evaluation IS NOT NULL 
                LIMIT 3
            """)
            sample_evaluations = cur.fetchall()
            
            cur.close()
            conn.close()
            
            print(f"\n=== Database Statistics (Minimal JSON Storage) ===")
            print(f"Total Records: {total_records}")
            print(f"Records with Evaluation Data: {with_evaluation}")
            print(f"Records with Parent Evaluation: {with_parent_evaluation}")
            print(f"Records with Sellers Data: {with_sellers}")
            
            print(f"\nSample Evaluation JSON Structure:")
            for record_id, bid_number, evaluation in sample_evaluations:
                print(f"  Bid: {bid_number} (ID: {record_id})")
                if evaluation:
                    eval_keys = list(evaluation.keys())
                    print(f"    JSON Keys: {eval_keys}")
                    sellers_count = len(evaluation.get('sellers_participated', []))
                    print(f"    Sellers Count: {sellers_count}")
                    print(f"    Has Financial: {evaluation.get('has_financial_evaluation', False)}")
                    print(f"    Has Technical: {evaluation.get('has_technical_evaluation', False)}")
                    print(f"    Has General: {evaluation.get('has_general_evaluation', False)}")
                    print()
            
            print(f"Top 5 Ministries:")
            for ministry, count in top_ministries:
                print(f"  {ministry}: {count} bids")
                
        except Exception as e:
            print(f"Error getting database stats: {e}")

def main():
    """Main function to run the scraper"""
    print("=== GeM Bid Scraper with PostgreSQL Storage (Minimal JSON) ===")
    print("Note: Make sure to update cookie_value and csrf_token in the code before running!")
    print("This version stores only essential evaluation data to reduce database size.")
    
    # Initialize scraper with hardcoded values
    scraper = GeMBidScraper()
    
    # Test database connection
    try:
        conn = psycopg2.connect(**scraper.db_config)
        conn.close()
        print("✓ Database connection successful")
    except Exception as e:
        print(f"✗ Database connection failed: {e}")
        print("Please check your database configuration in the code")
        return
    
    # Ask user for page range (optional customization)
    try:
        start_page = int(input("Enter start page (default: 1): ") or "1")
        end_page = int(input("Enter end page (default: 1000): ") or "1000")
    except ValueError:
        print("Invalid input, using default values: pages 1-1000")
        start_page, end_page = 1, 1000
    
    # Run the scraper
    try:
        bid_data = scraper.process_all_bids(start_page, end_page)
        
        # Show final database statistics
        scraper.get_database_stats()
        
    except KeyboardInterrupt:
        print("\n\nScraping interrupted by user")
    except Exception as e:
        print(f"\nError during scraping: {e}")
    
    print("\n=== Scraping Complete ===")
    print("Data has been saved to PostgreSQL database with minimal JSON structure")
    print("\nMinimal Evaluation JSON Structure:")
    print("- has_financial_evaluation: boolean")
    print("- has_technical_evaluation: boolean") 
    print("- has_general_evaluation: boolean")
    print("- sellers_participated: array of seller objects")
    print("- parent_bid_id_found: string (if found)")
    print("\nExcluded from database:")
    print("- financial_evaluation, technical_evaluation, general_evaluation arrays")
    print("- raw_html_length, extraction_method, has_sellers_list metadata")

if __name__ == "__main__":
    # Install required packages check
    try:
        import psycopg2
        from bs4 import BeautifulSoup
        print("✓ All required packages available")
    except ImportError as e:
        print(f"✗ Missing required package: {e}")
        print("Please install with:")
        print("pip install psycopg2-binary beautifulsoup4 lxml requests")
        exit(1)
    
    main()