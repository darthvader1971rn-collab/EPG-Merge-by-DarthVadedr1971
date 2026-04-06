import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import json
import os
import sqlite3
import gzip
import threading
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import glob
import lzma
import zipfile
import io
import argparse
import re

SETTINGS_FILE = 'settings.json'
DB_FILE = 'epg_cache.db'

class DatabaseManager:
    def __init__(self, db_path=DB_FILE):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.cursor = self.conn.cursor()
        self.setup_database()

    def setup_database(self):
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS channels (
                xml_id TEXT PRIMARY KEY,
                display_name TEXT,
                icon_url TEXT
            )
        ''')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS programs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_xml_id TEXT,
                start_time TEXT,
                stop_time TEXT,
                start_dt TEXT,
                stop_dt TEXT,
                title TEXT,
                desc TEXT,
                date TEXT,
                category TEXT,
                director TEXT,
                actors TEXT,
                rating TEXT,
                star_rating TEXT,
                icon_url TEXT,
                UNIQUE(channel_xml_id, start_dt)
            )
        ''')
        self.conn.commit()

    def clean_old_data(self):
        # Usuwamy dane starsze niż 7 dni, by baza była lekka
        limit_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
        self.cursor.execute("DELETE FROM programs WHERE stop_dt < ?", (limit_date,))
        self.conn.commit()

    def get_all_data_for_export(self):
        # Eksportujemy tylko to, co mieści się w oknie +/- 7 dni
        limit_past = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
        limit_future = (datetime.now() + timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
        
        self.cursor.execute("SELECT * FROM channels")
        channels = self.cursor.fetchall()
        
        self.cursor.execute("""
            SELECT * FROM programs 
            WHERE start_dt >= ? AND start_dt <= ? 
            ORDER BY channel_xml_id, start_dt
        """, (limit_past, limit_future))
        programs = self.cursor.fetchall()
        
        return channels, programs

class EPGProcessor:
    def __init__(self, urls, langs, output_dir, log_callback):
        self.urls = urls
        self.langs = [l.lower() for l in langs]
        self.output_dir = output_dir
        self.log = log_callback
        self.db = DatabaseManager()

    def normalize_id(self, xml_id):
        # Usuwa spacje, kropki i standaryzuje ID dla m3u
        if not xml_id: return ""
        return re.sub(r'[\s\.]', '', xml_id)

    def parse_xmltv_time(self, time_str):
        # Konwertuje czas XMLTV (20240320200000 +0100) na czytelny format bazy danych
        try:
            clean_time = time_str.split()[0]
            dt = datetime.strptime(clean_time, '%Y%m%d%H%M%S')
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except:
            return None

    def decompress_content(self, content, url):
        try:
            if url.lower().endswith('.gz') or content.startswith(b'\x1f\x8b'):
                return gzip.decompress(content)
            elif url.lower().endswith('.xz') or content.startswith(b'\xfd7zXZ\x00'):
                return lzma.decompress(content)
            elif url.lower().endswith('.zip') or content.startswith(b'PK\x03\x04'):
                with zipfile.ZipFile(io.BytesIO(content)) as z:
                    xml_names = [n for n in z.namelist() if n.lower().endswith('.xml')]
                    return z.read(xml_names[0]) if xml_names else z.read(z.namelist()[0])
        except Exception as e:
            self.log(f"Błąd dekompresji {url}: {e}")
        return content

    def run(self):
        self.log("Czyszczenie bazy i przygotowanie danych...")
        self.db.clean_old_data()
        
        master_channels = {}
        master_programs = {}

        for priority, url in enumerate(self.urls):
            if not url.strip(): continue
            self.log(f"Przetwarzanie źródła [{priority+1}]: {url}")
            try:
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                raw_content = self.decompress_content(response.content, url)
                root = ET.fromstring(raw_content)
                
                # 1. PARSOWANIE KANAŁÓW
                for channel in root.findall('channel'):
                    raw_id = channel.get('id')
                    ch_id = self.normalize_id(raw_id)
                    
                    if not ch_id: continue

                    display_name = channel.find('display-name').text if channel.find('display-name') is not None else raw_id
                    icon = channel.find('icon').get('src') if channel.find('icon') is not None else ""
                    
                    if ch_id not in master_channels:
                        master_channels[ch_id] = {'display_name': display_name, 'icon_url': icon}

                # 2. PARSOWANIE PROGRAMÓW
                for prog in root.findall('programme'):
                    raw_id = prog.get('channel')
                    ch_id = self.normalize_id(raw_id)
                    
                    if ch_id not in master_channels: continue

                    start = prog.get('start')
                    stop = prog.get('stop')
                    start_dt = self.parse_xmltv_time(start)
                    stop_dt = self.parse_xmltv_time(stop)
                    
                    if not start_dt: continue

                    # Wybór języka
                    title = ""
                    desc = ""
                    for t in prog.findall('title'):
                        lang = (t.get('lang') or "").lower()
                        if not self.langs or lang in self.langs or not title:
                            title = t.text
                    for d in prog.findall('desc'):
                        lang = (d.get('lang') or "").lower()
                        if not self.langs or lang in self.langs or not desc:
                            desc = d.text

                    # Jeśli brak polskiego tytułu w polskim EPG, pomijamy bałagan
                    if "pl" in self.langs and not title: continue

                    prog_key = (ch_id, start_dt) # Klucz oparty na znormalizowanym czasie
                    
                    if prog_key not in master_programs:
                        master_programs[prog_key] = {
                            'start_raw': start, 'stop_raw': stop,
                            'start_dt': start_dt, 'stop_dt': stop_dt,
                            'title': title, 'desc': desc,
                            'date': prog.findtext('date', ''),
                            'category': prog.findtext('category', ''),
                            'icon_url': prog.find('icon').get('src') if prog.find('icon') is not None else ""
                        }
            except Exception as e:
                self.log(f"Błąd źródła {url}: {e}")

        # ZAPIS DO BAZY
        self.log("Synchronizacja z bazą danych...")
        for ch_id, data in master_channels.items():
            self.db.cursor.execute("INSERT OR REPLACE INTO channels VALUES (?, ?, ?)", 
                                   (ch_id, data['display_name'], data['icon_url']))
            
        for (ch_id, s_dt), d in master_programs.items():
            self.db.cursor.execute('''
                INSERT OR REPLACE INTO programs 
                (channel_xml_id, start_time, stop_time, start_dt, stop_dt, title, desc, date, category)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (ch_id, d['start_raw'], d['stop_raw'], d['start_dt'], d['stop_dt'], 
                  d['title'], d['desc'], d['date'], d['category']))
        
        self.db.conn.commit()
        self.generate_export_file()

    def generate_export_file(self):
        channels, programs = self.db.get_all_data_for_export()
        tv = ET.Element("tv", {"generator-info-name": "EPG Master v2.0 Clean"})
        
        # Filtrujemy kanały - eksportujemy tylko te, które mają przypisane programy
        active_channel_ids = {p[1] for p in programs}
        
        for ch in channels:
            if ch[0] in active_channel_ids:
                ch_elem = ET.SubElement(tv, "channel", {"id": ch[0]})
                ET.SubElement(ch_elem, "display-name").text = ch[1]
                if ch[2]: ET.SubElement(ch_elem, "icon", {"src": ch[2]})
            
        for pr in programs:
            pr_elem = ET.SubElement(tv, "programme", {"channel": pr[1], "start": pr[2], "stop": pr[3]})
            ET.SubElement(pr_elem, "title", {"lang": "pl"}).text = pr[6] or "Brak tytułu"
            if pr[7]: ET.SubElement(pr_elem, "desc", {"lang": "pl"}).text = pr[7]
            if pr[9]: ET.SubElement(pr_elem, "category").text = pr[9]

        tree = ET.ElementTree(tv)
        if hasattr(ET, 'indent'): ET.indent(tree, space="  ")
            
        target_dir = os.path.join(self.output_dir, "Output")
        os.makedirs(target_dir, exist_ok=True)
        output_path = os.path.join(target_dir, "master_epg.xml.gz")
        
        with gzip.open(output_path, 'wb') as f:
            f.write(b'<?xml version="1.0" encoding="UTF-8"?>\n')
            tree.write(f, encoding='utf-8', xml_declaration=False)
            
        self.log(f"SUKCES! Wygenerowano przejrzyste EPG: {output_path}")

class App:
    def __init__(self, root):
        self.root = root
        self.root.title("EPG Master - Czysty Merge PL")
        self.root.geometry("800x600")
        self.url_frames = []
        self.lang_vars = {"pl": tk.BooleanVar(value=True), "en": tk.BooleanVar(), "de": tk.BooleanVar()}
        self.output_dir = tk.StringVar(value=os.getcwd())
        self.setup_ui()
        self.load_settings()

    def setup_ui(self):
        # UI pozostało zbliżone do Twojego, by zachować wygodę obsługi
        lang_frame = tk.LabelFrame(self.root, text="Języki (Tylko te będą w pliku końcowym)", padx=10, pady=5)
        lang_frame.pack(fill="x", padx=10, pady=5)
        for lang in self.lang_vars:
            tk.Checkbutton(lang_frame, text=lang.upper(), variable=self.lang_vars[lang]).pack(side="left")

        self.container = tk.Frame(self.root)
        self.container.pack(fill="both", expand=True, padx=10)
        
        btn_frame = tk.Frame(self.root)
        btn_frame.pack(fill="x", padx=10, pady=5)
        tk.Button(btn_frame, text="+ Dodaj źródło", command=self.add_url_row).pack(side="left")
        
        self.log_text = tk.Text(self.root, height=10, state="disabled", bg="#1e1e1e", fg="#00ff00")
        self.log_text.pack(fill="x", padx=10, pady=5)
        
        self.start_btn = tk.Button(self.root, text="GENERUJ CZYSTE EPG", font=("Arial", 12, "bold"), 
                                   bg="#2e7d32", fg="white", command=self.start_processing)
        self.start_btn.pack(fill="x", padx=10, pady=10)

    def add_url_row(self, url_val=""):
        row = tk.Frame(self.container)
        row.pack(fill="x", pady=2)
        entry = tk.Entry(row)
        entry.insert(0, url_val)
        entry.pack(side="left", fill="x", expand=True)
        tk.Button(row, text="X", fg="red", command=lambda: self.delete_row(row)).pack(side="right")
        self.url_frames.append(row)

    def delete_row(self, row):
        row.destroy()
        self.url_frames.remove(row)

    def start_processing(self):
        urls = [f.winfo_children()[0].get().strip() for f in self.url_frames if f.winfo_children()[0].get().strip()]
        langs = [k for k, v in self.lang_vars.items() if v.get()]
        
        self.start_btn.config(state="disabled", text="Pracuję...")
        threading.Thread(target=self.run_task, args=(urls, langs), daemon=True).start()

    def run_task(self, urls, langs):
        processor = EPGProcessor(urls, langs, self.output_dir.get(), self.log)
        processor.run()
        self.root.after(0, lambda: self.start_btn.config(state="normal", text="GENERUJ CZYSTE EPG"))

    def log(self, msg):
        self.log_text.config(state="normal")
        self.log_text.insert(tk.END, f"{msg}\n")
        self.log_text.see(tk.END)
        self.log_text.config(state="disabled")

    def save_settings(self):
        urls = [f.winfo_children()[0].get().strip() for f in self.url_frames]
        settings = {"urls": urls, "output_dir": self.output_dir.get()}
        with open(SETTINGS_FILE, 'w') as f: json.dump(settings, f)

    def load_settings(self):
        if os.path.exists(SETTINGS_FILE):
            with open(SETTINGS_FILE, 'r') as f:
                s = json.load(f)
                for u in s.get("urls", []): self.add_url_row(u)
        if not self.url_frames: self.add_url_row()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--cli', action='store_true')
    args = parser.parse_args()

    if args.cli:
        if os.path.exists(SETTINGS_FILE):
            with open(SETTINGS_FILE, 'r') as f:
                s = json.load(f)
                proc = EPGProcessor(s["urls"], ["pl"], os.getcwd(), print)
                proc.run()
    else:
        root = tk.Tk()
        app = App(root)
        root.mainloop()