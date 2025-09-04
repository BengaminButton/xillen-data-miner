import requests
import json
import csv
import sqlite3
import time
import threading
import queue
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import re
import os
import hashlib
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

author = "t.me/Bengamin_Button t.me/XillenAdapter"

class XillenDataMiner:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.config = {
            'max_threads': 10,
            'delay': 1,
            'timeout': 30,
            'max_pages': 1000,
            'output_format': 'json',
            'database_file': 'data_miner.db',
            'output_file': 'mined_data.json'
        }
        self.statistics = {
            'pages_scraped': 0,
            'data_extracted': 0,
            'errors': 0,
            'start_time': time.time()
        }
        self.scraped_urls = set()
        self.data_queue = queue.Queue()
        self.setup_database()
    
    def setup_database(self):
        self.conn = sqlite3.connect(self.config['database_file'])
        cursor = self.conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS scraped_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT,
                title TEXT,
                content TEXT,
                metadata TEXT,
                timestamp DATETIME,
                hash TEXT UNIQUE
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS urls (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT UNIQUE,
                status TEXT,
                timestamp DATETIME
            )
        ''')
        
        self.conn.commit()
    
    def add_url(self, url, status='pending'):
        cursor = self.conn.cursor()
        try:
            cursor.execute('INSERT OR IGNORE INTO urls (url, status, timestamp) VALUES (?, ?, ?)',
                         (url, status, datetime.now()))
            self.conn.commit()
        except sqlite3.IntegrityError:
            pass
    
    def update_url_status(self, url, status):
        cursor = self.conn.cursor()
        cursor.execute('UPDATE urls SET status = ? WHERE url = ?', (status, url))
        self.conn.commit()
    
    def save_scraped_data(self, url, title, content, metadata):
        content_hash = hashlib.md5(content.encode()).hexdigest()
        
        cursor = self.conn.cursor()
        try:
            cursor.execute('''
                INSERT OR IGNORE INTO scraped_data 
                (url, title, content, metadata, timestamp, hash) 
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (url, title, content, json.dumps(metadata), datetime.now(), content_hash))
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False
    
    def scrape_webpage(self, url):
        try:
            response = self.session.get(url, timeout=self.config['timeout'])
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            title = soup.find('title')
            title = title.get_text().strip() if title else ''
            
            content = soup.get_text()
            content = re.sub(r'\s+', ' ', content).strip()
            
            metadata = {
                'status_code': response.status_code,
                'content_type': response.headers.get('content-type', ''),
                'content_length': len(response.content),
                'links': [urljoin(url, link.get('href', '')) for link in soup.find_all('a', href=True)],
                'images': [urljoin(url, img.get('src', '')) for img in soup.find_all('img', src=True)],
                'forms': len(soup.find_all('form')),
                'tables': len(soup.find_all('table')),
                'scripts': len(soup.find_all('script')),
                'stylesheets': len(soup.find_all('link', rel='stylesheet'))
            }
            
            self.save_scraped_data(url, title, content, metadata)
            self.update_url_status(url, 'scraped')
            self.statistics['pages_scraped'] += 1
            self.statistics['data_extracted'] += 1
            
            return {
                'url': url,
                'title': title,
                'content': content,
                'metadata': metadata,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.update_url_status(url, 'error')
            self.statistics['errors'] += 1
            print(f"❌ Ошибка сканирования {url}: {e}")
            return None
    
    def extract_emails(self, text):
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        return re.findall(email_pattern, text)
    
    def extract_phones(self, text):
        phone_pattern = r'(\+?1[-.\s]?)?\(?([0-9]{3})\)?[-.\s]?([0-9]{3})[-.\s]?([0-9]{4})'
        return re.findall(phone_pattern, text)
    
    def extract_urls(self, text):
        url_pattern = r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
        return re.findall(url_pattern, text)
    
    def extract_social_media(self, text):
        social_patterns = {
            'twitter': r'@[A-Za-z0-9_]+',
            'instagram': r'@[A-Za-z0-9_.]+',
            'facebook': r'facebook\.com/[A-Za-z0-9_.]+',
            'linkedin': r'linkedin\.com/in/[A-Za-z0-9_.-]+'
        }
        
        social_data = {}
        for platform, pattern in social_patterns.items():
            matches = re.findall(pattern, text)
            if matches:
                social_data[platform] = matches
        
        return social_data
    
    def extract_financial_data(self, text):
        financial_patterns = {
            'credit_cards': r'\b(?:\d{4}[-\s]?){3}\d{4}\b',
            'ssn': r'\b\d{3}-\d{2}-\d{4}\b',
            'bitcoin': r'\b[13][a-km-zA-HJ-NP-Z1-9]{25,34}\b',
            'ethereum': r'\b0x[a-fA-F0-9]{40}\b'
        }
        
        financial_data = {}
        for data_type, pattern in financial_patterns.items():
            matches = re.findall(pattern, text)
            if matches:
                financial_data[data_type] = matches
        
        return financial_data
    
    def analyze_content(self, content):
        analysis = {
            'word_count': len(content.split()),
            'character_count': len(content),
            'sentences': len(re.split(r'[.!?]+', content)),
            'paragraphs': len(content.split('\n\n')),
            'emails': self.extract_emails(content),
            'phones': self.extract_phones(content),
            'urls': self.extract_urls(content),
            'social_media': self.extract_social_media(content),
            'financial_data': self.extract_financial_data(content),
            'keywords': self.extract_keywords(content)
        }
        
        return analysis
    
    def extract_keywords(self, text, min_length=3, max_keywords=20):
        words = re.findall(r'\b[a-zA-Z]+\b', text.lower())
        word_freq = {}
        
        for word in words:
            if len(word) >= min_length:
                word_freq[word] = word_freq.get(word, 0) + 1
        
        sorted_words = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)
        return [word for word, freq in sorted_words[:max_keywords]]
    
    def crawl_website(self, start_url, max_depth=2):
        urls_to_visit = [(start_url, 0)]
        visited_urls = set()
        
        while urls_to_visit and len(visited_urls) < self.config['max_pages']:
            current_url, depth = urls_to_visit.pop(0)
            
            if current_url in visited_urls or depth > max_depth:
                continue
            
            visited_urls.add(current_url)
            self.add_url(current_url)
            
            print(f"🔍 Сканирование: {current_url} (глубина: {depth})")
            
            data = self.scrape_webpage(current_url)
            if data:
                analysis = self.analyze_content(data['content'])
                data['analysis'] = analysis
                
                if depth < max_depth:
                    for link in data['metadata']['links']:
                        if link not in visited_urls and self.is_valid_url(link):
                            urls_to_visit.append((link, depth + 1))
            
            time.sleep(self.config['delay'])
        
        return list(visited_urls)
    
    def is_valid_url(self, url):
        try:
            parsed = urlparse(url)
            return bool(parsed.netloc) and parsed.scheme in ['http', 'https']
        except:
            return False
    
    def scrape_multiple_urls(self, urls):
        results = []
        
        with ThreadPoolExecutor(max_workers=self.config['max_threads']) as executor:
            future_to_url = {executor.submit(self.scrape_webpage, url): url for url in urls}
            
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    result = future.result()
                    if result:
                        results.append(result)
                except Exception as e:
                    print(f"❌ Ошибка обработки {url}: {e}")
        
        return results
    
    def export_to_json(self, filename):
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM scraped_data ORDER BY timestamp DESC')
        rows = cursor.fetchall()
        
        data = []
        for row in rows:
            data.append({
                'id': row[0],
                'url': row[1],
                'title': row[2],
                'content': row[3],
                'metadata': json.loads(row[4]) if row[4] else {},
                'timestamp': row[5],
                'hash': row[6]
            })
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"✅ Данные экспортированы в {filename}")
    
    def export_to_csv(self, filename):
        cursor = self.conn.cursor()
        cursor.execute('SELECT url, title, content, timestamp FROM scraped_data ORDER BY timestamp DESC')
        rows = cursor.fetchall()
        
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['URL', 'Title', 'Content', 'Timestamp'])
            writer.writerows(rows)
        
        print(f"✅ Данные экспортированы в {filename}")
    
    def search_data(self, query, limit=100):
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT url, title, content, timestamp 
            FROM scraped_data 
            WHERE title LIKE ? OR content LIKE ? 
            ORDER BY timestamp DESC 
            LIMIT ?
        ''', (f'%{query}%', f'%{query}%', limit))
        
        results = cursor.fetchall()
        return results
    
    def get_statistics(self):
        cursor = self.conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM scraped_data')
        total_data = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM urls')
        total_urls = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM urls WHERE status = "scraped"')
        scraped_urls = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM urls WHERE status = "error"')
        error_urls = cursor.fetchone()[0]
        
        uptime = time.time() - self.statistics['start_time']
        
        return {
            'total_data': total_data,
            'total_urls': total_urls,
            'scraped_urls': scraped_urls,
            'error_urls': error_urls,
            'uptime': uptime,
            'pages_scraped': self.statistics['pages_scraped'],
            'data_extracted': self.statistics['data_extracted'],
            'errors': self.statistics['errors']
        }
    
    def show_statistics(self):
        stats = self.get_statistics()
        
        print(f"\n📊 Статистика майнера данных:")
        print(f"   Автор: {author}")
        print(f"   Время работы: {stats['uptime']:.2f} сек")
        print(f"   Всего данных: {stats['total_data']}")
        print(f"   Всего URL: {stats['total_urls']}")
        print(f"   Просканировано: {stats['scraped_urls']}")
        print(f"   Ошибок: {stats['error_urls']}")
        print(f"   Страниц обработано: {stats['pages_scraped']}")
        print(f"   Данных извлечено: {stats['data_extracted']}")
    
    def show_menu(self):
        print(f"\n⛏️  Xillen Data Miner")
        print(f"👨‍💻 Автор: {author}")
        print(f"\nОпции:")
        print(f"1. Сканировать веб-сайт")
        print(f"2. Сканировать список URL")
        print(f"3. Поиск в данных")
        print(f"4. Экспорт в JSON")
        print(f"5. Экспорт в CSV")
        print(f"6. Показать статистику")
        print(f"7. Настройки")
        print(f"8. Очистить базу данных")
        print(f"0. Выход")
    
    def interactive_mode(self):
        while True:
            self.show_menu()
            choice = input("\nВыберите опцию: ").strip()
            
            try:
                if choice == '1':
                    url = input("Введите URL для сканирования: ").strip()
                    max_depth = int(input("Максимальная глубина (по умолчанию 2): ") or 2)
                    self.crawl_website(url, max_depth)
                
                elif choice == '2':
                    urls_input = input("Введите URL через запятую: ").strip()
                    urls = [url.strip() for url in urls_input.split(',')]
                    results = self.scrape_multiple_urls(urls)
                    print(f"✅ Обработано {len(results)} URL")
                
                elif choice == '3':
                    query = input("Введите поисковый запрос: ").strip()
                    results = self.search_data(query)
                    print(f"\n🔍 Найдено {len(results)} результатов:")
                    for i, (url, title, content, timestamp) in enumerate(results[:10]):
                        print(f"{i+1}. {title} - {url}")
                
                elif choice == '4':
                    filename = input("Имя файла (по умолчанию mined_data.json): ").strip()
                    if not filename:
                        filename = self.config['output_file']
                    self.export_to_json(filename)
                
                elif choice == '5':
                    filename = input("Имя файла (по умолчанию mined_data.csv): ").strip()
                    if not filename:
                        filename = 'mined_data.csv'
                    self.export_to_csv(filename)
                
                elif choice == '6':
                    self.show_statistics()
                
                elif choice == '7':
                    print(f"\n⚙️  Настройки:")
                    print(f"   Максимальные потоки: {self.config['max_threads']}")
                    print(f"   Задержка: {self.config['delay']} сек")
                    print(f"   Таймаут: {self.config['timeout']} сек")
                    print(f"   Максимум страниц: {self.config['max_pages']}")
                    print(f"   Файл базы данных: {self.config['database_file']}")
                
                elif choice == '8':
                    confirm = input("Вы уверены? (y/n): ").strip().lower()
                    if confirm == 'y':
                        cursor = self.conn.cursor()
                        cursor.execute('DELETE FROM scraped_data')
                        cursor.execute('DELETE FROM urls')
                        self.conn.commit()
                        print("✅ База данных очищена")
                
                elif choice == '0':
                    print("👋 До свидания!")
                    break
                
                else:
                    print("❌ Неверный выбор")
            
            except Exception as e:
                print(f"❌ Ошибка: {e}")
    
    def close(self):
        if hasattr(self, 'conn'):
            self.conn.close()

def main():
    print(author)
    
    miner = XillenDataMiner()
    
    if len(sys.argv) > 1:
        if sys.argv[1] == 'crawl' and len(sys.argv) > 2:
            url = sys.argv[2]
            max_depth = int(sys.argv[3]) if len(sys.argv) > 3 else 2
            miner.crawl_website(url, max_depth)
            miner.export_to_json('crawl_results.json')
        elif sys.argv[1] == 'scrape' and len(sys.argv) > 2:
            urls = sys.argv[2].split(',')
            results = miner.scrape_multiple_urls(urls)
            print(f"✅ Обработано {len(results)} URL")
        else:
            print("Использование:")
            print("  python main.py crawl <url> [max_depth]")
            print("  python main.py scrape <url1,url2,url3>")
    else:
        try:
            miner.interactive_mode()
        finally:
            miner.close()

if __name__ == "__main__":
    import sys
    main()
