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
                UNIQUE(channel_xml_id, start_time)
            )
        ''')
        self.conn.commit()

    def clean_old_catchup(self):
        limit_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
        self.cursor.execute("DELETE FROM programs WHERE stop_dt < ?", (limit_date,))
        self.conn.commit()

    def get_all_data_for_export(self):
        limit_past = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
        limit_future = (datetime.now() + timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
        
        self.cursor.execute("SELECT * FROM channels")
        channels = self.cursor.fetchall()
        
        self.cursor.execute("SELECT * FROM programs WHERE start_dt >= ? AND start_dt <= ? ORDER BY channel_xml_id, start_dt", (limit_past, limit_future))
        programs = self.cursor.fetchall()
        
        return channels, programs

class EPGProcessor:
    def __init__(self, urls, langs, output_dir, log_callback):
        self.urls = urls
        self.langs = langs
        self.output_dir = output_dir
        self.log = log_callback
        self.db = DatabaseManager()

    def parse_time(self, time_str):
        try:
            clean_str = time_str.split(' ')[0]
            return datetime.strptime(clean_str, '%Y%m%d%H%M%S')
        except:
            return datetime.now()

    def decompress_content(self, content, url):
        try:
            if url.lower().endswith('.gz') or content.startswith(b'\x1f\x8b'):
                return gzip.decompress(content)
            elif url.lower().endswith('.xz') or content.startswith(b'\xfd7zXZ\x00'):
                return lzma.decompress(content)
            elif url.lower().endswith('.zip') or content.startswith(b'PK\x03\x04'):
                with zipfile.ZipFile(io.BytesIO(content)) as z:
                    xml_names = [n for n in z.namelist() if n.lower().endswith('.xml')]
                    if xml_names:
                        return z.read(xml_names[0])
                    elif z.namelist():
                        return z.read(z.namelist()[0])
        except Exception as e:
            self.log(f"Ostrzeżenie: Błąd podczas próby dekompresji pliku z {url}: {e}")
        
        return content

    def run(self):
        self.log("Rozpoczynam czyszczenie starego archiwum CatchUp (starsze niż 7 dni)...")
        self.db.clean_old_catchup()
        
        master_channels = {}
        master_programs = {}

        for priority, url in enumerate(self.urls):
            if not url.strip(): continue
            self.log(f"Pobieranie źródła [{priority+1}]: {url}")
            try:
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                
                self.log(f"Rozpakowywanie i parsowanie źródła [{priority+1}]...")
                raw_content = self.decompress_content(response.content, url)
                root = ET.fromstring(raw_content)
                
                for channel in root.findall('channel'):
                    ch_id = channel.get('id')
                    display_name = channel.find('display-name').text if channel.find('display-name') is not None else ch_id
                    icon = channel.find('icon').get('src') if channel.find('icon') is not None else ""
                    
                    if ch_id not in master_channels:
                        master_channels[ch_id] = {'display_name': display_name, 'icon_url': icon}
                    elif priority > 0 and not master_channels[ch_id]['icon_url'] and icon:
                        master_channels[ch_id]['icon_url'] = icon

                for prog in root.findall('programme'):
                    ch_id = prog.get('channel')
                    start = prog.get('start')
                    stop = prog.get('stop')
                    
                    title = ""
                    desc = ""
                    for t in prog.findall('title'):
                        lang = t.get('lang', '').lower()
                        if not self.langs or lang in self.langs or title == "":
                            title = t.text
                    for d in prog.findall('desc'):
                        lang = d.get('lang', '').lower()
                        if not self.langs or lang in self.langs or desc == "":
                            desc = d.text

                    date = prog.find('date').text if prog.find('date') is not None else ""
                    category = prog.find('category').text if prog.find('category') is not None else ""
                    rating = prog.find('rating/value').text if prog.find('rating') is not None and prog.find('rating/value') is not None else ""
                    star_rating = prog.find('star-rating/value').text if prog.find('star-rating') is not None and prog.find('star-rating/value') is not None else ""
                    icon = prog.find('icon').get('src') if prog.find('icon') is not None else ""
                    
                    director = ""
                    actors = []
                    credits_tag = prog.find('credits')
                    if credits_tag is not None:
                        dir_tag = credits_tag.find('director')
                        if dir_tag is not None: director = dir_tag.text
                        for actor in credits_tag.findall('actor'):
                            actors.append(actor.text)
                    actors_str = ",".join(actors) if actors else ""

                    prog_key = (ch_id, start)
                    
                    if prog_key not in master_programs:
                        start_dt = self.parse_time(start)
                        stop_dt = self.parse_time(stop)
                        master_programs[prog_key] = {
                            'stop': stop, 
                            'start_dt': start_dt.strftime('%Y-%m-%d %H:%M:%S'), 
                            'stop_dt': stop_dt.strftime('%Y-%m-%d %H:%M:%S'),
                            'title': title, 'desc': desc, 'date': date, 'category': category,
                            'director': director, 'actors': actors_str, 'rating': rating,
                            'star_rating': star_rating, 'icon_url': icon
                        }
                    else:
                        mp = master_programs[prog_key]
                        if not mp['date']: mp['date'] = date
                        if not mp['category']: mp['category'] = category
                        if not mp['director']: mp['director'] = director
                        if not mp['actors']: mp['actors'] = actors_str
                        if not mp['rating']: mp['rating'] = rating
                        if not mp['star_rating']: mp['star_rating'] = star_rating
                        if not mp['icon_url']: mp['icon_url'] = icon
                        if not mp['desc'] and desc: mp['desc'] = desc

            except Exception as e:
                self.log(f"Błąd podczas przetwarzania {url}: {str(e)}")

        self.log("Zapisywanie połączonych danych do bazy SQLite...")
        for ch_id, data in master_channels.items():
            self.db.cursor.execute("INSERT OR REPLACE INTO channels (xml_id, display_name, icon_url) VALUES (?, ?, ?)", 
                                   (ch_id, data['display_name'], data['icon_url']))
            
        for (ch_id, start), data in master_programs.items():
            self.db.cursor.execute('''
                INSERT OR REPLACE INTO programs 
                (channel_xml_id, start_time, stop_time, start_dt, stop_dt, title, desc, date, category, director, actors, rating, star_rating, icon_url)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (ch_id, start, data['stop'], data['start_dt'], data['stop_dt'], data['title'], data['desc'], 
                  data['date'], data['category'], data['director'], data['actors'], data['rating'], data['star_rating'], data['icon_url']))
        
        self.db.conn.commit()
        self.generate_export_file()

    def generate_export_file(self):
        self.log("Generowanie pliku wynikowego master_epg.xml.gz...")
        channels, programs = self.db.get_all_data_for_export()
        
        tv = ET.Element("tv", {"generator-info-name": "EPG Master Python Script"})
        
        for ch in channels:
            ch_elem = ET.SubElement(tv, "channel", {"id": ch[0]})
            ET.SubElement(ch_elem, "display-name").text = ch[1]
            if ch[2]: ET.SubElement(ch_elem, "icon", {"src": ch[2]})
            
        for pr in programs:
            pr_elem = ET.SubElement(tv, "programme", {"channel": pr[1], "start": pr[2], "stop": pr[3]})
            ET.SubElement(pr_elem, "title").text = pr[6] or "Brak tytułu"
            if pr[7]: ET.SubElement(pr_elem, "desc").text = pr[7]
            if pr[8]: ET.SubElement(pr_elem, "date").text = pr[8]
            if pr[9]: ET.SubElement(pr_elem, "category").text = pr[9]
            
            if pr[10] or pr[11]:
                credits_elem = ET.SubElement(pr_elem, "credits")
                if pr[10]: ET.SubElement(credits_elem, "director").text = pr[10]
                if pr[11]:
                    for actor in pr[11].split(','):
                        if actor: ET.SubElement(credits_elem, "actor").text = actor
                        
            if pr[12]:
                rating_elem = ET.SubElement(pr_elem, "rating")
                ET.SubElement(rating_elem, "value").text = pr[12]
            if pr[13]:
                star_elem = ET.SubElement(pr_elem, "star-rating")
                ET.SubElement(star_elem, "value").text = pr[13]
            if pr[14]: ET.SubElement(pr_elem, "icon", {"src": pr[14]})

        # Formatowanie XML (Pretty-Print) przed kompresją
        tree = ET.ElementTree(tv)
        if hasattr(ET, 'indent'):
            ET.indent(tree, space="  ", level=0)
            
        # Wymuszenie zapisu do folderu Output
        target_dir = os.path.join(self.output_dir, "Output") if os.path.basename(self.output_dir) != "Output" else self.output_dir
        os.makedirs(target_dir, exist_ok=True)
        
        output_path = os.path.join(target_dir, "master_epg.xml.gz")
        
        with gzip.open(output_path, 'wb') as f:
            tree.write(f, encoding='utf-8', xml_declaration=True)
            
        self.log(f"Zakończono sukcesem! Zapisano do: {output_path}")

class App:
    def __init__(self, root):
        self.root = root
        self.root.title("EPG Master - Super Merge")
        self.root.geometry("850x650")
        
        self.url_frames = []
        self.lang_vars = {"pl": tk.BooleanVar(), "en": tk.BooleanVar(), "de": tk.BooleanVar()}
        self.custom_lang = tk.StringVar()
        self.output_dir = tk.StringVar(value=os.getcwd())
        
        self.setup_ui()
        self.load_settings()

    def setup_ui(self):
        lang_frame = tk.LabelFrame(self.root, text="Preferowane Języki", padx=10, pady=5)
        lang_frame.pack(fill="x", padx=10, pady=5)
        
        tk.Checkbutton(lang_frame, text="PL", variable=self.lang_vars["pl"], command=self.save_settings).pack(side="left", padx=5)
        tk.Checkbutton(lang_frame, text="EN", variable=self.lang_vars["en"], command=self.save_settings).pack(side="left", padx=5)
        tk.Checkbutton(lang_frame, text="DE", variable=self.lang_vars["de"], command=self.save_settings).pack(side="left", padx=5)
        
        tk.Label(lang_frame, text="Inny kod:").pack(side="left", padx=5)
        entry_custom_lang = tk.Entry(lang_frame, textvariable=self.custom_lang, width=5)
        entry_custom_lang.pack(side="left")
        entry_custom_lang.bind("<FocusOut>", lambda e: self.save_settings())

        self.links_outer_frame = tk.LabelFrame(self.root, text="Źródła EPG (Kolejność określa priorytet - nr 1 jest najważniejszy)", padx=5, pady=5)
        self.links_outer_frame.pack(fill="both", expand=True, padx=10, pady=5)

        self.canvas = tk.Canvas(self.links_outer_frame, highlightthickness=0)
        self.scrollbar = ttk.Scrollbar(self.links_outer_frame, orient="vertical", command=self.canvas.yview)
        
        self.links_container = tk.Frame(self.canvas)
        
        self.canvas_window = self.canvas.create_window((0, 0), window=self.links_container, anchor="nw")
        
        self.links_container.bind("<Configure>", self._on_frame_configure)
        self.canvas.bind("<Configure>", self._on_canvas_configure)
        
        self.canvas.configure(yscrollcommand=self.scrollbar.set)
        
        self.canvas.pack(side="left", fill="both", expand=True)
        self.scrollbar.pack(side="right", fill="y")
        
        self.canvas.bind_all("<MouseWheel>", self._on_mousewheel)

        ctrl_frame = tk.Frame(self.root)
        ctrl_frame.pack(fill="x", padx=10, pady=5)
        tk.Button(ctrl_frame, text="+ Dodaj puste pole", command=self.add_url_row).pack(side="left", padx=5)
        tk.Button(ctrl_frame, text="Importuj folder (epgimport)", command=self.import_from_folder).pack(side="left", padx=5)
        
        save_frame = tk.Frame(self.root)
        save_frame.pack(fill="x", padx=10, pady=5)
        tk.Label(save_frame, text="Folder docelowy:").pack(side="left")
        tk.Entry(save_frame, textvariable=self.output_dir, state="readonly", width=50).pack(side="left", padx=5)
        tk.Button(save_frame, text="Wybierz...", command=self.choose_dir).pack(side="left")

        self.log_text = tk.Text(self.root, height=10, state="disabled", bg="#f0f0f0")
        self.log_text.pack(fill="x", padx=10, pady=5)
        
        self.start_btn = tk.Button(self.root, text="🚀 ROZPOCZNIJ MERGE 🚀", font=("Arial", 12, "bold"), bg="#4CAF50", fg="white", command=self.start_processing)
        self.start_btn.pack(fill="x", padx=10, pady=10)

    def _on_frame_configure(self, event=None):
        self.canvas.configure(scrollregion=self.canvas.bbox("all"))

    def _on_canvas_configure(self, event):
        self.canvas.itemconfig(self.canvas_window, width=event.width)

    def _on_mousewheel(self, event):
        self.canvas.yview_scroll(int(-1*(event.delta/120)), "units")

    def add_url_row(self, url_val=""):
        if self.url_frames:
            last_entry = self.url_frames[-1].winfo_children()[2].get().strip()
            if not last_entry and not url_val:
                messagebox.showwarning("Uwaga", "Wypełnij najpierw puste pole linkiem, zanim dodasz kolejne.")
                return

        row_frame = tk.Frame(self.links_container)
        row_frame.pack(fill="x", pady=2)
        
        lbl_drag = tk.Label(row_frame, text=" ☰ ", cursor="sb_v_double_arrow", fg="gray")
        lbl_drag.pack(side="left", padx=(0, 5))
        lbl_drag.bind("<Button-1>", lambda e, f=row_frame: self._on_drag_start(e, f))
        lbl_drag.bind("<ButtonRelease-1>", lambda e, f=row_frame: self._on_drag_release(e, f))
        
        lbl = tk.Label(row_frame, text=f"Nr {len(self.url_frames) + 1}", width=5)
        lbl.pack(side="left")
        
        entry = tk.Entry(row_frame)
        entry.insert(0, url_val)
        entry.pack(side="left", fill="x", expand=True, padx=5)
        entry.bind("<FocusOut>", lambda e: self.save_settings())
        
        btn_up = tk.Button(row_frame, text="↑", command=lambda f=row_frame: self.move_row(f, -1))
        btn_up.pack(side="left")
        btn_down = tk.Button(row_frame, text="↓", command=lambda f=row_frame: self.move_row(f, 1))
        btn_down.pack(side="left")
        
        btn_del = tk.Button(row_frame, text="X", fg="red", command=lambda f=row_frame: self.delete_row(f))
        btn_del.pack(side="left", padx=5)
        
        self.url_frames.append(row_frame)
        self.update_labels()
        self.save_settings()
        
        self.canvas.update_idletasks()
        self.canvas.yview_moveto(1.0)

    def _on_drag_start(self, event, frame):
        pass

    def _on_drag_release(self, event, dragged_frame):
        mouse_y = event.y_root
        new_idx = 0
        
        for i, frame in enumerate(self.url_frames):
            frame_y = frame.winfo_rooty()
            frame_h = frame.winfo_height()
            if mouse_y > frame_y + (frame_h / 2):
                new_idx = i + 1
                
        old_idx = self.url_frames.index(dragged_frame)
        if new_idx > old_idx:
            new_idx -= 1
            
        new_idx = max(0, min(new_idx, len(self.url_frames) - 1))
        
        if old_idx != new_idx:
            self.url_frames.insert(new_idx, self.url_frames.pop(old_idx))
            self._repack_frames()
            self.save_settings()

    def delete_row(self, frame):
        frame.destroy()
        self.url_frames.remove(frame)
        self.update_labels()
        self.save_settings()

    def move_row(self, frame, direction):
        idx = self.url_frames.index(frame)
        new_idx = idx + direction
        if 0 <= new_idx < len(self.url_frames):
            self.url_frames[idx], self.url_frames[new_idx] = self.url_frames[new_idx], self.url_frames[idx]
            self._repack_frames()
            self.save_settings()

    def _repack_frames(self):
        for f in self.url_frames:
            f.pack_forget()
        for f in self.url_frames:
            f.pack(fill="x", pady=2)
        self.update_labels()

    def update_labels(self):
        for idx, frame in enumerate(self.url_frames):
            lbl = frame.winfo_children()[1]
            lbl.config(text=f"Nr {idx + 1}")

    def import_from_folder(self):
        folder = filedialog.askdirectory(title="Wybierz folder z plikami XML (epgimport)")
        if not folder: return
        
        count = 0
        for filepath in glob.glob(os.path.join(folder, "*.xml")):
            try:
                tree = ET.parse(filepath)
                root = tree.getroot()
                for source in root.findall('.//source'):
                    url_tag = source.find('url')
                    if url_tag is not None and url_tag.text:
                        self.add_url_row(url_tag.text.strip())
                        count += 1
            except Exception as e:
                self.log(f"Błąd parsowania pliku {filepath}: {e}")
                
        messagebox.showinfo("Import", f"Zaimportowano {count} linków z folderu.")

    def choose_dir(self):
        dir_path = filedialog.askdirectory()
        if dir_path:
            self.output_dir.set(dir_path)
            self.save_settings()

    def get_current_urls(self):
        urls = []
        for frame in self.url_frames:
            entry = frame.winfo_children()[2]
            if entry.get().strip():
                urls.append(entry.get().strip())
        return urls

    def save_settings(self):
        settings = {
            "urls": self.get_current_urls(),
            "langs": {k: v.get() for k, v in self.lang_vars.items()},
            "custom_lang": self.custom_lang.get(),
            "output_dir": self.output_dir.get()
        }
        with open(SETTINGS_FILE, 'w', encoding='utf-8') as f:
            json.dump(settings, f, indent=4)

    def load_settings(self):
        if os.path.exists(SETTINGS_FILE):
            try:
                with open(SETTINGS_FILE, 'r', encoding='utf-8') as f:
                    settings = json.load(f)
                    
                for url in settings.get("urls", []):
                    self.add_url_row(url)
                    
                for k, v in settings.get("langs", {}).items():
                    if k in self.lang_vars: self.lang_vars[k].set(v)
                    
                self.custom_lang.set(settings.get("custom_lang", ""))
                
                # Zabezpieczenie przed błędem jeśli z GitHuba skrypt wgra stary katalog
                saved_dir = settings.get("output_dir", os.getcwd())
                if os.path.exists(saved_dir):
                    self.output_dir.set(saved_dir)
                else:
                    self.output_dir.set(os.getcwd())
                    
            except Exception as e:
                self.log(f"Nie udało się wczytać ustawień: {e}")
        
        if not self.url_frames:
            self.add_url_row()

    def log(self, message):
        self.log_text.config(state="normal")
        self.log_text.insert(tk.END, f"{datetime.now().strftime('%H:%M:%S')} - {message}\n")
        self.log_text.see(tk.END)
        self.log_text.config(state="disabled")

    def start_processing(self):
        urls = self.get_current_urls()
        if not urls:
            messagebox.showerror("Błąd", "Brak linków do przetworzenia.")
            return

        active_langs = [k for k, v in self.lang_vars.items() if v.get()]
        if self.custom_lang.get().strip():
            active_langs.append(self.custom_lang.get().strip().lower())

        self.start_btn.config(state="disabled", text="Przetwarzanie w tle...")
        
        thread = threading.Thread(target=self.run_merge_task, args=(urls, active_langs))
        thread.daemon = True
        thread.start()

    def run_merge_task(self, urls, langs):
        processor = EPGProcessor(urls, langs, self.output_dir.get(), self.log)
        processor.run()
        self.root.after(0, lambda: self.start_btn.config(state="normal", text="🚀 ROZPOCZNIJ MERGE 🚀"))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="EPG Master - Super Merge")
    parser.add_argument('--cli', action='store_true', help='Uruchamia skrypt w trybie konsolowym (Headless) dla GitHub Actions')
    args = parser.parse_args()

    if args.cli:
        print("Uruchamianie EPG Master w trybie konsolowym (Headless)...")
        if os.path.exists(SETTINGS_FILE):
            with open(SETTINGS_FILE, 'r', encoding='utf-8') as f:
                settings = json.load(f)
            
            urls = settings.get("urls", [])
            langs = [k for k, v in settings.get("langs", {}).items() if v]
            custom_lang = settings.get("custom_lang", "").strip().lower()
            if custom_lang: langs.append(custom_lang)
            
            out_dir = os.getcwd() # W GitHub Actions wymuszamy aktualny katalog
            
            if not urls:
                print("Błąd: Plik settings.json nie zawiera żadnych linków.")
            else:
                processor = EPGProcessor(urls, langs, out_dir, print)
                processor.run()
        else:
            print(f"Błąd: Brak pliku {SETTINGS_FILE}! Skonfiguruj program najpierw w trybie okienkowym i wyślij plik na repozytorium.")
    else:
        root = tk.Tk()
        app = App(root)
        root.mainloop()
