import os
import httpx
import json
import time
from bs4 import BeautifulSoup
from utils import html2text
from dotenv import load_dotenv
from datetime import datetime, timedelta
import sqlite3
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

LOGIN_URL = "https://discourse.onlinedegree.iitm.ac.in/login"
BASE_URL = "https://discourse.onlinedegree.iitm.ac.in"
USERNAME = os.getenv("DISCOURSE_USER")
PASSWORD = os.getenv("DISCOURSE_PASS")

@dataclass
class DiscoursePost:
    post_id: str
    topic_id: str
    title: str
    content: str
    markdown_content: str
    author: str
    created_at: str
    url: str
    category: str
    tags: List[str]
    reply_count: int
    post_number: int

class EnhancedTDSScraper:
    def __init__(self, db_path: str = "tds_data.db"):
        self.client = httpx.Client(follow_redirects=True, timeout=30.0)
        self.db_path = db_path
        self.init_database()
        
        # Date range for TDS project (Jan 1 - Apr 14, 2025)
        self.start_date = datetime(2025, 1, 1)
        self.end_date = datetime(2025, 4, 14)
        
        # TDS-related keywords for filtering
        self.tds_keywords = [
            'tds', 'tools in data science', 'tools-in-data-science',
            'python', 'pandas', 'numpy', 'matplotlib', 'seaborn',
            'git', 'github', 'jupyter', 'assignment', 'graded',
            'quiz', 'project', 'week', 'ga1', 'ga2', 'ga3', 'ga4', 'ga5',
            'programming', 'data science', 'analytics'
        ]
    
    def init_database(self):
        """Initialize SQLite database for storing scraped data"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS discourse_posts (
                post_id TEXT PRIMARY KEY,
                topic_id TEXT,
                title TEXT,
                content TEXT,
                markdown_content TEXT,
                author TEXT,
                created_at TEXT,
                url TEXT,
                category TEXT,
                tags TEXT,
                reply_count INTEGER,
                post_number INTEGER,
                scraped_at TEXT
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def login(self):
        """Login to Discourse"""
        try:
            resp = self.client.get(LOGIN_URL)
            logger.info(f"Login page status: {resp.status_code}")
            soup = BeautifulSoup(resp.text, "html.parser")
            csrf_token= soup.find("meta", {"name": "csrf-token"})

            
            if not csrf_token:
                logger.error("Could not find CSRF token")
                return False
                
            csrf = csrf_token["value"]
            
            login_data = {
                "login": USERNAME,
                "password": PASSWORD,
                "authenticity_token": csrf
            }
            
            login_resp = self.client.post(LOGIN_URL, data=login_data)
            logger.info(f"Login response status: {login_resp.status_code}")
            
            # Check if login was successful
            if login_resp.status_code == 200:
                logger.info("Login successful")
                return True
            else:
                logger.error("Login failed")
                return False
                
        except Exception as e:
            logger.error(f"Login error: {e}")
            return False
    
    def get_categories(self) -> List[Dict]:
        """Get all categories from Discourse"""
        try:
            response = self.client.get(f"{BASE_URL}/categories.json")
            response.raise_for_status()
            data = response.json()
            
            categories = data.get('category_list', {}).get('categories', [])
            
            # Filter for TDS-related categories
            tds_categories = []
            for category in categories:
                name = category.get('name', '').lower()
                slug = category.get('slug', '').lower()
                description = category.get('description', '').lower()
                
                if any(keyword in name or keyword in slug or keyword in description 
                      for keyword in self.tds_keywords):
                    tds_categories.append(category)
                    logger.info(f"Found TDS category: {name} ({slug})")
            
            return tds_categories
            
        except Exception as e:
            logger.error(f"Error fetching categories: {e}")
            return []
    
    def is_within_date_range(self, date_str: str) -> bool:
        """Check if date is within scraping range"""
        try:
            date_obj = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            return self.start_date <= date_obj <= self.end_date
        except:
            return True  # Include if can't parse date
    
    def is_tds_related(self, title: str, content: str = "", tags: List[str] = None) -> bool:
        """Check if topic/post is TDS related"""
        text_to_check = f"{title} {content}".lower()
        tags_to_check = " ".join(tags or []).lower()
        
        return any(keyword in text_to_check or keyword in tags_to_check 
                  for keyword in self.tds_keywords)
    
    def scrape_topics_from_category(self, category_slug: str, category_name: str, limit: int = None) -> int:
        """Scrape topics from a specific category"""
        scraped_count = 0
        page = 0
        
        while True:
            try:
                url = f"{BASE_URL}/c/{category_slug}.json"
                if page > 0:
                    url += f"?page={page}"
                
                response = self.client.get(url)
                response.raise_for_status()
                data = response.json()
                
                topics = data.get("topic_list", {}).get("topics", [])
                
                if not topics:
                    break
                
                logger.info(f"Processing page {page} of category {category_name} ({len(topics)} topics)")
                
                for topic in topics:
                    # Check date range
                    if not self.is_within_date_range(topic.get('created_at', '')):
                        continue
                    
                    # Check if TDS related
                    title = topic.get('title', '')
                    tags = topic.get('tags', [])
                    
                    if not self.is_tds_related(title, tags=tags):
                        continue
                    
                    # Scrape individual topic
                    topic_scraped = self.scrape_topic(topic, category_name)
                    if topic_scraped:
                        scraped_count += topic_scraped
                    
                    # Respect rate limits
                    time.sleep(0.5)
                    
                    # Check limit
                    if limit and scraped_count >= limit:
                        return scraped_count
                
                page += 1
                
                # Safety limit to prevent infinite loops
                if page > 100:
                    break
                    
            except Exception as e:
                logger.error(f"Error scraping category {category_slug}, page {page}: {e}")
                break
        
        return scraped_count
    
    def scrape_topic(self, topic_data: Dict, category_name: str) -> int:
        """Scrape individual topic and all its posts"""
        topic_id = topic_data.get("id")
        slug = topic_data.get("slug")
        title = topic_data.get("title", "")
        
        try:
            # Get topic details with all posts
            topic_url = f"{BASE_URL}/t/{topic_id}.json"
            response = self.client.get(topic_url)
            response.raise_for_status()
            topic_details = response.json()
            
            posts = topic_details.get("post_stream", {}).get("posts", [])
            scraped_posts = 0
            
            for post_data in posts:
                try:
                    # Get HTML version for markdown conversion
                    html_url = f"{BASE_URL}/t/{slug}/{topic_id}/{post_data.get('post_number', 1)}"
                    html_response = self.client.get(html_url)
                    markdown_content = html2text(html_response.text) if html_response.status_code == 200 else ""
                    
                    # Create post object
                    post = DiscoursePost(
                        post_id=str(post_data.get('id')),
                        topic_id=str(topic_id),
                        title=title,
                        content=self.clean_html_content(post_data.get('cooked', '')),
                        markdown_content=markdown_content,
                        author=post_data.get('username', ''),
                        created_at=post_data.get('created_at', ''),
                        url=html_url,
                        category=category_name,
                        tags=topic_data.get('tags', []),
                        reply_count=topic_data.get('reply_count', 0),
                        post_number=post_data.get('post_number', 1)
                    )
                    
                    # Save to database
                    self.save_post(post)
                    
                    # Save markdown file
                    self.save_markdown_file(post, slug)
                    
                    scraped_posts += 1
                    
                except Exception as e:
                    logger.error(f"Error processing post {post_data.get('id')}: {e}")
            
            logger.info(f"Scraped topic '{title}': {scraped_posts} posts")
            return scraped_posts
            
        except Exception as e:
            logger.error(f"Error scraping topic {topic_id}: {e}")
            return 0
    
    def clean_html_content(self, html_content: str) -> str:
        """Clean HTML content and extract text"""
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Remove script and style elements
        for script in soup(["script", "style"]):
            script.decompose()
        
        # Get text and clean up
        text = soup.get_text()
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        text = ' '.join(chunk for chunk in chunks if chunk)
        
        return text
    
    def save_post(self, post: DiscoursePost):
        """Save post to database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO discourse_posts 
            (post_id, topic_id, title, content, markdown_content, author, created_at, 
             url, category, tags, reply_count, post_number, scraped_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            post.post_id, post.topic_id, post.title, post.content,
            post.markdown_content, post.author, post.created_at, post.url,
            post.category, json.dumps(post.tags), post.reply_count,
            post.post_number, datetime.now().isoformat()
        ))
        
        conn.commit()
        conn.close()
    
    def save_markdown_file(self, post: DiscoursePost, slug: str):
        """Save post as markdown file"""
        os.makedirs("data/markdown", exist_ok=True)
        
        filename = f"data/markdown/{slug}_post_{post.post_number}.md"
        
        with open(filename, "w", encoding="utf-8") as f:
            f.write(f"# {post.title}\n\n")
            f.write(f"**Author:** {post.author}\n")
            f.write(f"**Date:** {post.created_at}\n")
            f.write(f"**URL:** {post.url}\n")
            f.write(f"**Category:** {post.category}\n")
            f.write(f"**Tags:** {', '.join(post.tags)}\n\n")
            f.write("---\n\n")
            f.write(post.markdown_content)
    
    def scrape_all_tds_content(self, limit_per_category: int = None):
        """Main method to scrape all TDS-related content"""
        if not self.login():
            logger.error("Failed to login. Cannot proceed with scraping.")
            return
        
        logger.info("Starting TDS content scraping...")
        
        # Get TDS categories
        categories = self.get_categories()
        
        if not categories:
            logger.warning("No TDS categories found. Trying common category slugs...")
            # Try common TDS category slugs
            common_slugs = ['tools-in-data-science', 'tds', 'programming', 'data-science']
            categories = [{'slug': slug, 'name': slug.replace('-', ' ').title()} 
                         for slug in common_slugs]
        
        total_scraped = 0
        
        for category in categories:
            category_slug = category.get('slug')
            category_name = category.get('name', category_slug)
            
            logger.info(f"Scraping category: {category_name}")
            
            try:
                scraped = self.scrape_topics_from_category(
                    category_slug, 
                    category_name, 
                    limit_per_category
                )
                total_scraped += scraped
                logger.info(f"Scraped {scraped} posts from {category_name}")
                
            except Exception as e:
                logger.error(f"Error scraping category {category_name}: {e}")
        
        logger.info(f"Scraping completed. Total posts scraped: {total_scraped}")
        
        # Generate summary
        self.generate_scraping_summary()
    
    def generate_scraping_summary(self):
        """Generate a summary of scraped data"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get statistics
        cursor.execute("SELECT COUNT(*) FROM discourse_posts")
        total_posts = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT topic_id) FROM discourse_posts")
        total_topics = cursor.fetchone()[0]
        
        cursor.execute("SELECT category, COUNT(*) FROM discourse_posts GROUP BY category")
        category_counts = cursor.fetchall()
        
        cursor.execute("SELECT author, COUNT(*) FROM discourse_posts GROUP BY author ORDER BY COUNT(*) DESC LIMIT 10")
        top_authors = cursor.fetchall()
        
        conn.close()
        
        # Write summary
        summary = f"""# TDS Discourse Scraping Summary

## Overview
- **Total Posts:** {total_posts}
- **Total Topics:** {total_topics}
- **Date Range:** {self.start_date.strftime('%Y-%m-%d')} to {self.end_date.strftime('%Y-%m-%d')}

## Posts by Category
"""
        
        for category, count in category_counts:
            summary += f"- **{category}:** {count} posts\n"
        
        summary += "\n## Top Authors\n"
        for author, count in top_authors[:5]:
            summary += f"- **{author}:** {count} posts\n"
        
        with open("data/scraping_summary.md", "w", encoding="utf-8") as f:
            f.write(summary)
        
        logger.info("Scraping summary saved to data/scraping_summary.md")

# Usage functions for backward compatibility
def login():
    scraper = EnhancedTDSScraper()
    return scraper.login()

def scrape_topics(category_slug, limit=50):
    """Enhanced version of your original function"""
    scraper = EnhancedTDSScraper()
    if scraper.login():
        return scraper.scrape_topics_from_category(category_slug, category_slug, limit)
    return 0

# Main execution
if __name__ == "__main__":
    scraper = EnhancedTDSScraper()
    
    # Scrape all TDS content
    scraper.scrape_all_tds_content(limit_per_category=100)
    
    print("Scraping completed! Check the database and markdown files in data/ directory.")