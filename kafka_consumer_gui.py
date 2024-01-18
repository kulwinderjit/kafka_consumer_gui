from time import sleep
import time
from tkinter import _flatten, _join, _stringify, _splitdict
from tkinter import filedialog
from tkinter import messagebox
from tkinter import *
from tkinter.ttk import *
from kafka import KafkaConsumer
from datetime import datetime, timezone
from kafka.errors import NoBrokersAvailable, KafkaTimeoutError
from typing import List, Dict
import sqlite3
from tkinter import colorchooser
import json
import os
import threading
import collections
db_name = 'consumer_config.db'
version = "4"
total_lines_limit = 20000

ConsumerRecord = collections.namedtuple("ConsumerRecord",
    ["topic", "partition", "offset", "timestamp", "timestamp_type",
     "key", "value", "headers", "checksum", "serialized_key_size", "serialized_value_size", "serialized_header_size"])

class ConsumerUI:
    def __init__(self, tab, root):
        self.consumer_after_id = None
        self.consumer = None
        self.thread = None
        self.tab = tab
        self.main_window = root
        self.highligher_rules: List[Dict] = []
        tab.bind("<Destroy>", self.on_closing_frame)
        buttons_frame = Frame(self.tab)
        about_button = Button(buttons_frame, text='About', command=self.show_about)
        about_button.grid(row=0, column=3, sticky=N+E, padx=5, pady=5)
        servers_label = LabelFrame(buttons_frame, text='Bootstrap servers (comma separated)', padding='2 2 2 2')
        self.servers = Combobox(servers_label)
        self.servers.grid(row=0, column=0, sticky=N+W+E)
        servers_label.grid(row=0, column=0, padx=5, sticky=N+W+E)
        servers_label.columnconfigure(0, weight=1)

        topic_label = LabelFrame(buttons_frame, text='Topic (comma separated)', padding='2 2 2 2')
        self.topic = Combobox(topic_label)
        self.topic.grid(row=0, column=0, sticky=N+W+E)
        topic_label.grid(row=0, column=1, padx=5, sticky=N+W+E)
        topic_label.columnconfigure(0, weight=1)

        message_filter_label = LabelFrame(buttons_frame, text='Message filters (, for OR ; for AND)', padding='2 2 2 2')
        self.message_filter = Entry(message_filter_label, textvariable=StringVar())
        self.message_filter.grid(row=0, column=0, sticky=S+W+E)
        message_filter_label.grid(row=2, column=0, padx=5, sticky=S+W+E)
        message_filter_label.columnconfigure(0, weight=1)

        file_label = LabelFrame(buttons_frame, text='File', padding='2 2 2 2')
        self.file = Combobox(file_label, state='readonly')
        self.file.grid(row=0, column=0, sticky=N+W+E)
        file_label.grid(row=1, column=1, padx=5, sticky=N+W+E)
        file_label.columnconfigure(0, weight=1)

        file_buttons_frame = Frame(buttons_frame)
        self.browse_button = Button(file_buttons_frame, text='Browse..', command= lambda: self.browse(self.file))
        self.browse_button.grid(row=0, column=1, sticky=S+W)
        self.open_button = Button(file_buttons_frame, text='Open', command= lambda: self.sysopen(self.file))
        self.open_button.grid(row=0, column=2, sticky=S+W)
        file_buttons_frame.grid(row=1, column=2, sticky=S+W)

        type_label = LabelFrame(buttons_frame, text='Fetch type', padding='2 2 2 2')
        self.type = Combobox(type_label, state='readonly')
        self.type.grid(row=0, column=0, sticky=N+W+E)
        type_label.grid(row=1, column=0, padx=5, sticky=N+W+E)
        type_label.columnconfigure(0, weight=1)
        self.type['values'] = ('latest', 'earliest')
        self.type.current(0)

        control_buttons_frame = Frame(buttons_frame)
        highlight_button  = Button(control_buttons_frame, text='Highlight', command=self.configure_highlight)
        highlight_button.grid(row=0, column=0, padx=5)
        self.consume_button = Button(control_buttons_frame, text='Consume', command= lambda: self.start_consumer())
        self.consume_button.grid(row=0, column=1, padx=5, sticky=S+W)
        clr_button = Button(control_buttons_frame, text='Clear', command = lambda: self.value.delete('1.0', END))
        clr_button.grid(row=0, column=2, sticky=W+S, padx=5)
        self.scroll_lock_button_var = IntVar()
        self.include_extra_fields = IntVar()
        scroll_lock_button = Checkbutton(control_buttons_frame, text='ScrollLock', variable=self.scroll_lock_button_var)
        scroll_lock_button.grid(row=0, column=4, sticky=W+S, padx=5)
        extra_button = Checkbutton(control_buttons_frame, text='Extra Headers', variable=self.include_extra_fields)
        extra_button.grid(row=0, column=5, sticky=W+S, padx=5)
        self.json_format_filter_var = IntVar()
        json_format_filter = Checkbutton(control_buttons_frame, text='Format Json', variable=self.json_format_filter_var)
        json_format_filter.grid(row=0, column=3, sticky=S+W)
        control_buttons_frame.grid(row=2, column=1, sticky=S+W+E)

        buttons_frame.grid(row=0, column=0, sticky=N+W+E)
        buttons_frame.columnconfigure(0, weight=1)
        buttons_frame.columnconfigure(1, weight=1)
        buttons_frame.columnconfigure(2, weight=1)
        buttons_frame.columnconfigure(3, weight=1)

        value_label = LabelFrame(self.tab, text='Output', padding='2 2 2 2')
        self.value = Text(value_label, wrap=NONE, undo=False, autoseparators=True)
        scrollb = Scrollbar(value_label, command=self.value.yview)
        scrollb_h = Scrollbar(value_label, command=self.value.xview, orient=HORIZONTAL)
        self.value['yscrollcommand'] = scrollb.set
        self.value['xscrollcommand'] = scrollb_h.set
        self.value.grid(row=0, column=0, padx=2, pady=2, sticky=N+W+E+S)
        value_label.grid(row=1, column=0, padx=5, sticky=N+W+E+S)
        value_label.columnconfigure(0, weight=1)
        value_label.rowconfigure(0, weight=1)
        scrollb.grid(row=0, column=1, sticky=N+S)
        scrollb_h.grid(row=1, column=0, sticky=E+W)

        self.tab.rowconfigure(0, weight=1)
        self.tab.rowconfigure(1, weight=16)
        self.tab.columnconfigure(0, weight=1)
        self.update_lists()
        self.initiate_highlight()
    def controls_state(self, state: str):
        self.servers['state'] = state
        self.topic['state'] = state
        self.file['state'] = state
        self.type['state'] = state
        self.browse_button['state'] = state
    class About:
        def __init__(self, parent):
            self.tp = Toplevel(parent, background='black')
            self.tp.transient(parent)
            self.tp.grab_set()
            self.tp.title('About')
            w = 300
            h = 100
            self.tp.resizable(0, 0)
            ws = parent.winfo_screenwidth() # width of the screen
            hs = parent.winfo_screenheight() # height of the screen
            x = (ws/2) - (w/2)
            y = (hs/2) - (h/2)
            self.tp.geometry('%dx%d+%d+%d' % (w, h, x, y))
            self.tp.bind("<Return>", self.ok)
            self.tp.bind("<Escape>", self.ok)
            l = Label(self.tp, text='Created by Kulwinderjit Singh', anchor=CENTER, font='Helvetica 14', foreground='green', background='black')
            l.pack()
            l2 = Label(self.tp, text='in python', anchor=CENTER, font='Helvetica 14', foreground='green', background='black')
            l2.pack()
            l2 = Label(self.tp, text='Version ' + version, anchor=CENTER, font='Helvetica 14', foreground='green', background='black')
            l2.pack()
        def ok(self, event=None):
            self.tp.destroy()

    class Highlighter:
        def __init__(self, parent, options: List[Dict]):
            self.font = 'Helvetica 12'
            self.options = options
            self.tp = Toplevel(parent)
            self.tp.transient(parent)
            self.tp.grab_set()
            self.ok_done = None
            self.tp.title('Configure highlights')
            w = 500
            h = 400
            self.tp.resizable(0, 0)
            ws = parent.winfo_screenwidth() # width of the screen
            hs = parent.winfo_screenheight() # height of the screen
            x = (ws/2) - (w/2)
            y = (hs/2) - (h/2)
            self.tp.geometry('%dx%d+%d+%d' % (w, h, x, y))
            self.tp.bind("<Return>", self.ok)
            controls_frame = Frame(self.tp)
            listbox_frame = Frame(controls_frame)
            self.listbox = Listbox(listbox_frame, height=10, activestyle='dotbox', font=self.font)
            self.listbox.bind("<Return>", self.ok)
            self.listbox.bind("<Escape>", self.cancel)
            self.listbox.grid(row=0, column=0, sticky=N+W+E+S, pady=10)
            self.listbox.bind("<<ListboxSelect>>", self.on_list_select)
            for item in self.options:
                self.listbox.insert(END, item['txt'])
                self.listbox.itemconfig(self.listbox.size()-1, {'bg':item['bg'], 'fg': item['fg']})
            scrollbar = Scrollbar(listbox_frame)
            scrollbar.grid(row=0, column=1, sticky=N+S)
            listbox_frame.columnconfigure(0, weight=1)
            listbox_frame.rowconfigure(0, weight=1)
            listbox_frame.grid(row=0, column=0, sticky=N+S+W+E)
            self.listbox.config(yscrollcommand = scrollbar.set) 
            scrollbar.config(command = self.listbox.yview) 
            self.listbox.focus_set()
            buttons_frame = Frame(controls_frame)
            add_button = Button(buttons_frame, text = 'Add', command=self.add_item)
            add_button.grid(row=0, column=0, sticky=W)
            remove_button = Button(buttons_frame, text = 'Delete', command=self.delete_item)
            remove_button.grid(row=0, column=1, sticky=W)
            up_button = Button(buttons_frame, text = 'Move Up', command=self.move_up)
            up_button.grid(row=0, column=2, sticky=E)
            down_button = Button(buttons_frame, text = 'Move Down', command=self.move_down)
            down_button.grid(row=0, column=3, sticky=E)
            buttons_frame.columnconfigure(0, weight=1)
            buttons_frame.columnconfigure(1, weight=1)
            buttons_frame.columnconfigure(2, weight=1)
            buttons_frame.columnconfigure(3, weight=1)
            buttons_frame.rowconfigure(0, weight=1)
            buttons_frame.grid(row=1, column=0, sticky=W+E, pady=10, padx=10)

            colors_frame = Frame(controls_frame)
            self.fg_label = Label(colors_frame, text='Sample highlighted text', anchor='center', font=self.font, foreground='red', background='white')
            fg_button = Button(colors_frame, text='Foreground color', command=self.select_fg)
            bg_button = Button(colors_frame, text='Background color', command=self.select_bg)
            self.fg_label.grid(row=0, column=0, sticky=W+E)
            fg_button.grid(row=0, column=1, sticky=W+E)
            bg_button.grid(row=0, column=2, sticky=W+E)
            colors_frame.columnconfigure(0, weight=1)
            colors_frame.columnconfigure(1, weight=1)
            colors_frame.columnconfigure(2, weight=1)
            colors_frame.grid(row=2, column=0, sticky=W+E, pady=10, padx=10)

            txt_frame = LabelFrame(controls_frame, text='String:')
            self.txt_input = Entry(txt_frame)
            self.txt_input.bind('<FocusIn>', self.text_focus)
            self.txt_input.grid(row=0, column=0, sticky=W+E, pady=5, padx=5)
            txt_frame.grid(row=3, column=0, sticky=W+E, pady=5, padx=5)
            txt_frame.columnconfigure(0, weight=1)

            form_buttons_frame = Frame(controls_frame)
            ok_btn = Button(form_buttons_frame, text='Ok', command=self.ok)
            cancel_btn = Button(form_buttons_frame, text='Cancel', command=self.cancel)
            ok_btn.grid(row=0, column=0, sticky=W, padx=10)
            cancel_btn.grid(row=0, column=1, sticky=E)
            form_buttons_frame.columnconfigure(0, weight=1)
            form_buttons_frame.columnconfigure(1, weight=1)
            form_buttons_frame.grid(row=5, column=0, sticky=E, padx=20, pady=10)
            controls_frame.grid(row=0, column=0, sticky=N+W+E+S)
            controls_frame.columnconfigure(0, weight=1)
            controls_frame.rowconfigure(0, weight=1)
            self.tp.rowconfigure(0, weight=1)
            self.tp.columnconfigure(0, weight=1)
        def text_focus(self, event):
            selected = self.listbox.curselection()
            if len(selected) > 0:
                self.listbox.selection_clear(selected[0])
        def on_list_select(self, event):
            _selected = self.listbox.curselection()
            if len(_selected)> 0:
                selected = _selected[0]
                bg = self.listbox.itemcget(selected, 'background')
                fg = self.listbox.itemcget(selected, 'foreground')
                self.fg_label.configure(background=bg, foreground=fg)
        def select_bg(self):
            color_code = colorchooser.askcolor(title ="Choose background color")
            if color_code:
                self.fg_label.configure(background=color_code[1])
                selected = self.listbox.curselection()
                if len(selected):
                    sel_idx = selected[0]
                    self.listbox.itemconfig(sel_idx, {'bg': color_code[1]})
        def select_fg(self):
            color_code = colorchooser.askcolor(title ="Choose foreground color")
            if color_code:
                self.fg_label.configure(foreground=color_code[1])
                selected = self.listbox.curselection()
                if len(selected):
                    sel_idx = selected[0]
                    self.listbox.itemconfig(sel_idx, {'fg': color_code[1]})
        def move_up(self):
            selected = self.listbox.curselection()
            if len(selected):
                sel_idx = selected[0]
                if sel_idx > 0:
                    txt = self.listbox.get(sel_idx)
                    bg = self.listbox.itemcget(sel_idx, 'background')
                    fg = self.listbox.itemcget(sel_idx, 'foreground')
                    self.listbox.delete(sel_idx)
                    sel_idx = sel_idx - 1
                    self.listbox.insert(sel_idx, txt)
                    self.listbox.itemconfig(sel_idx, {'bg': bg, 'fg': fg})
                    self.listbox.selection_set(sel_idx)
        def move_down(self):
            selected = self.listbox.curselection()
            if len(selected):
                sel_idx = selected[0]
                if sel_idx < self.listbox.size()-1:
                    txt = self.listbox.get(sel_idx)
                    bg = self.listbox.itemcget(sel_idx, 'background')
                    fg = self.listbox.itemcget(sel_idx, 'foreground')
                    self.listbox.delete(sel_idx)
                    sel_idx = sel_idx + 1
                    self.listbox.insert(sel_idx, txt)
                    self.listbox.itemconfig(sel_idx, {'bg': bg, 'fg': fg})
                    self.listbox.selection_set(sel_idx)
        def delete_item(self):
            selected = self.listbox.curselection()
            if len(selected):
                sel = selected[0]
                self.listbox.delete(sel)
                if sel > 0:
                    self.listbox.selection_set(sel-1)
                else:
                    self.listbox.selection_set(0)
        def add_item(self):
            txt = self.txt_input.get()
            if len(txt.lstrip()) > 0:
                self.listbox.insert(END, self.txt_input.get())
                self.listbox.itemconfig(self.listbox.size()-1, {'bg':self.fg_label['background'], 'fg': self.fg_label['foreground']})
            self.txt_input.focus_set()
        def ok(self, event=None):
            self.options.clear()
            for i in range(self.listbox.size()):
                txt = self.listbox.get(i)
                bg = self.listbox.itemcget(i, 'background')
                fg = self.listbox.itemcget(i, 'foreground')
                self.options.append({'idx':str(i), 'txt':txt, 'fg':fg, 'bg':bg})
            self.ok_done = True
            self.tp.destroy()
    
        def cancel(self, event=None):
            self.tp.destroy()

    def append_message(self, key, message):
        format = self.json_format_filter_var.get()
        line = None
        if format == 1:
            try:
                message = json.loads(message)
                line = str.format('{} : {}{}{}', key, '\n', json.dumps(message, indent=2), '\n')
            except:
                line = str.format('{} : {}{}', key, message, '\n')
        else:
            line = str.format('{} : {}{}', key, message, '\n')
        tag = self.get_tag_name(line)
        if tag:
            self.append_text(line, tag)
        else:
            self.append_text(line)
        if self.scroll_lock_button_var.get() == 0:
            self.value.see(END)
    def get_tag_name(self, line: str):
        for rule in self.highligher_rules:
            if rule['txt'] in line:
                return rule['idx']
        return None
    def get_key(self, msg: ConsumerRecord) -> str:
        if msg.key is not None:
            try:
                key = msg.key.decode()
            except:
                key = msg.key.hex()
        else:
            key = "null"
        return key
    def get_kafka_headers(self, msg: ConsumerRecord) -> str:
        ts_str = 'null'
        if msg.timestamp is not None:
            ts = msg.timestamp/1000.0
            ts_str = datetime.fromtimestamp(ts, tz = timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[0:23]
        return f"Partition: {msg.partition} Offset: {msg.offset} Timestamp: {ts_str} Key: {self.get_key(msg)}"
    def consume_multi(self, consumer: KafkaConsumer):
        while(not consumer._closed):
            extra_fields = self.include_extra_fields.get()
            filter = str(self.message_filter.get())
            or_filters = filter.split(',')
            or_filters = [f for f in or_filters if len(f) > 0]
            and_filters = filter.split(';')
            and_filters = [f for f in and_filters if len(f) > 0]
            for msg in consumer:
                try:
                    if extra_fields == 1:
                        key = self.get_kafka_headers(msg)
                    else:
                        key = self.get_key(msg)
                except:
                    key = 'null'
                try:
                    message = msg.value.decode()
                except:
                    message = msg.value.hex()
                if len(and_filters) > 1:
                    if all(f in message or f in key for f in and_filters):
                        self.append_message(key, message)
                elif len(or_filters) > 0:
                    if any(f in message or f in key for f in or_filters):
                        self.append_message(key, message)
                else:
                    self.append_message(key, message)
    def consume(self, consumer):
        t = threading.Thread(target=self.consume_multi, args=(consumer, ))
        t.setDaemon(True)
        t.start()
        self.thread = t

    def write_to_file(self, csf, key, message):
        format = self.json_format_filter_var.get()
        line = None
        if format == 1:
            try:
                message = json.loads(message)
                line = str.format('{} : {}{}', key, '\n', json.dumps(message, indent=2))
            except:
                line = str.format('{} : {}{}', key, message, '\n')
        else:
            line = str.format('{} : {}{}', key, message, '\n')
        csf.writelines([line])

    def consume_to_file(self, consumer, file):
        self.append_text('Started consuming to file : ' + str(datetime.now()) + '\n')
        extra_fields = self.include_extra_fields.get()
        filter = str(self.message_filter.get())
        or_filters = filter.split(',')
        or_filters = [f for f in or_filters if len(f) > 0]
        and_filters = filter.split(';')
        and_filters = [f for f in and_filters if len(f) > 0]
        with open(file,'w',newline='') as csf:
            for msg in consumer:
                if extra_fields == 1:
                    key = self.get_kafka_headers(msg)
                else:
                    key = self.get_key(msg)
                try:
                    message = msg.value.decode()
                except:
                    message = msg.value.hex()
                try:
                    message = msg.value.decode()
                except:
                    message = msg.value.hex()
                if len(and_filters) > 1:
                    if all(f in message or f in key for f in and_filters):
                        self.write_to_file(csf, key, message)
                elif len(or_filters) > 0:
                    if any(f in message or f in key for f in or_filters):
                        self.write_to_file(csf, key, message)
                else:
                    self.write_to_file(csf, key, message)
                self.main_window.update()
        self.append_text('Consume to file completed: ' + str(datetime.now()) + '\n')
        self.value.see(END)

    def append_text(self, val, tag = None):
        if tag is None:
            self.value.insert(END, val)
        else:
            self.value.insert(END, val, tag)
        self.limit_text_value()
        
    def limit_text_value(self):
        total_no_of_lines = int(self.value.index('end-1c').split('.')[0])
        if total_no_of_lines >= total_lines_limit:
            self.value.delete("1.0", f'{total_no_of_lines - total_lines_limit}.0')

    def get_kafka_consumer(self, servers, type, timeout):
        consumer = KafkaConsumer(bootstrap_servers=servers,auto_offset_reset=type, consumer_timeout_ms=timeout, check_crcs=False)
        return consumer

    def consume_to_kafka(self, servers, topics, type, file):
        topis_list = str(topics).split(',')
        try:
            if self.consumer is not None:
                self.consumer.unsubscribe()
                self.consumer.close()
                self.consume_button.config(text='Consume')
                self.consumer = None
                self.thread = None
                self.controls_state('normal')
                return
            if type == 'latest':
                self.main_window.update()
                self.consumer = self.get_kafka_consumer(servers, type, 100)
                self.main_window.update()
                self.consumer.subscribe(topis_list)
                self.main_window.update()
                self.consume(self.consumer)
                self.append_text('Started consuming ' + topics + ' : '+ str(datetime.now()) + '\n')
                self.value.see(END)
                self.consume_button.config(text='Stop Consuming')
                self.controls_state('disabled')
            else:
                self.main_window.update()
                valid, file = self.validate_file_path(file)
                if not valid:
                    messagebox.showwarning(title='Select file first', message=file)
                    return
                self.consumer = self.get_kafka_consumer(servers, type, 2000)
                self.main_window.update()
                self.consumer.subscribe(topis_list)
                self.main_window.update()
                self.consume_button.config(text='Stop Consuming')
                self.controls_state('disabled')
                self.main_window.update()
                self.consume_to_file(self.consumer, file)
                self.consume_button.config(text='Consume')
                self.controls_state('normal')
                self.main_window.update()
                if self.consumer is not None:
                    self.consumer.close()
                self.consumer = None
        except NoBrokersAvailable as e:
            self.append_text(str(datetime.now()) + ': ' + str(e) + '\n')
            self.value.see(END)
        except KafkaTimeoutError as e:
            self.append_text(str(datetime.now()) + ': ' + str(e) + '\n')
            self.value.see(END)
        except ValueError as e:
            self.append_text(str(datetime.now()) + ': ' + str(e) + '\n')
            self.value.see(END)

    def validate_file_path(self, file):
        if len(file) <= 0:
            return (False, 'File path is required when fetch type is earliest.')
        if os.path.exists(file):
            if os.path.isdir(file):
                file_name = str(datetime.now()).replace(':','.').replace(' ', '_') + '.txt'
                file = file + '/' + file_name
                return (True, file)
            elif os.path.isfile(file):
                return (True, file)
            else:
                return (False, 'Path is neither a directory or a file')
        return (True, file)

    def get_highlighter(self):
        l = []
        with SqlliteConn(db_name=db_name) as conn:
            cursor = conn.execute('select idx, txt, bg, fg from highlighter order by idx asc')
            for r in cursor:
                l.append({'idx': str(r[0]), 'txt':r[1], 'bg': r[2], 'fg': r[3]})
        return l
    def set_highlighter(self, filters: List[Dict]):
        with SqlliteConn(db_name=db_name) as conn:
            conn.execute('delete from highlighter')
        with SqlliteConn(db_name=db_name) as conn:
            idx: int = 0
            for filter in filters:
                conn.execute('insert into highlighter(idx, txt, bg, fg) values(?,?,?,?)', (idx, filter['txt'], filter['bg'], filter['fg']))
                idx += 1
    def get_list(self, type):
        l = []
        with SqlliteConn(db_name=db_name) as conn:
            cursor = conn.execute('select value from %s order by timestamp desc' % type)
            for r in cursor:
                l.append(r[0])
        return l

    def relenght_tables(self):
        maxlen = 20
        with SqlliteConn(db_name=db_name) as conn:
            conn.execute('delete from brokers where timestamp in (select b.timestamp from brokers b left join (select timestamp from brokers order by timestamp desc limit ' + str(maxlen) + ') b1 on b1.timestamp = b.timestamp where b1.timestamp is null);')
            conn.execute('delete from topics where timestamp in (select b.timestamp from topics b left join (select timestamp from topics order by timestamp desc limit ' + str(maxlen) + ') b1 on b1.timestamp = b.timestamp where b1.timestamp is null);')
            conn.execute('delete from files where timestamp in (select b.timestamp from files b left join (select timestamp from files order by timestamp desc limit ' + str(maxlen) + ') b1 on b1.timestamp = b.timestamp where b1.timestamp is null);')

    def start_consumer(self):
        _broker = str(self.servers.get()).strip()
        _topic = str(self.topic.get()).strip()
        _file = str(self.file.get()).strip()
        _type = str(self.type.get()).strip()
        if len(_broker) == 0 or len(_topic) == 0:
            messagebox.showwarning(title='Fields required',message='Broker and topic fields are required')
            return
        with SqlliteConn(db_name=db_name) as conn:
            conn.execute('insert or replace into brokers(value) values(?)', (_broker,))
            conn.execute('insert or replace into topics(value) values(?)', (_topic,))
            if len(_file) > 0:
                conn.execute('insert or replace into files(value) values(?)', (_file,))
        self.relenght_tables()
        self.update_lists()
        self.consume_to_kafka(_broker, _topic, _type, _file)

    def get_latest_msg(self):
        with SqlliteConn(db_name=db_name) as conn:
            cursor = conn.execute('select value,timestamp from messages order by timestamp desc limit 1')
            for r in cursor:
                return (r[0], r[1])
        return (None,None)

    def update_lists(self):
        self.servers['values'] = self.get_list('brokers')
        if len(self.servers['values'])>0:
            self.servers.current(0)
        self.topic['values'] = self.get_list('topics')
        if len(self.topic['values'])>0:
            self.topic.current(0)
        self.file['values'] = self.get_list('files')
        if len(self.file['values'])>0:
            self.file.current(0)

    def sysopen(self, file_box: Combobox):
        filetxt = file_box.get()
        if len(str.lstrip(filetxt))>0:
            os.startfile(filetxt, 'open')

    def browse(self, file_box):
        filename = filedialog.asksaveasfilename(confirmoverwrite=True)
        if len(filename) == 0:
            return
        values = self.file['values']
        if len(values) ==0:
            self.file['values'] = (filename)
        else:
            self.file['values'] = (filename, ) + values
        self.file.current(0)

    def on_closing_frame(self, event):
        if self.consumer is not None:
            self.consumer.close()
            self.thread.join(0.3)
    def show_about(self):
        d = self.About(self.main_window)
        self.main_window.wait_window(d.tp)
    def configure_highlight(self):
        d = self.Highlighter(self.main_window, self.highligher_rules)
        self.main_window.wait_window(d.tp)
        if d.ok_done:
            self.set_highlighter(self.highligher_rules)
            for rule in self.highligher_rules:
                self.value.tag_configure(rule['idx'], background=rule['bg'], foreground=rule['fg'], selectforeground=rule['bg'], selectbackground=rule['fg'])
    def initiate_highlight(self):
        _highligher_rules = self.get_highlighter()
        for rule in _highligher_rules:
            self.highligher_rules.append(rule)
            self.value.tag_configure(rule['idx'], background=rule['bg'], foreground=rule['fg'], selectforeground=rule['bg'], selectbackground=rule['fg'])
    
class SqlliteConn(): 
    def __init__(self, db_name): 
        self.db_name = db_name 
        self.connection = None

    def __enter__(self): 
        self.connection = sqlite3.connect(self.db_name, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        return self.connection

    def __exit__(self, exc_type, exc_value, exc_traceback): 
        self.connection.commit()
        self.connection.close()

def _format_optvalue(value, script=False):
    """Internal function."""
    if script:
        # if caller passes a Tcl script to tk.call, all the values need to
        # be grouped into words (arguments to a command in Tcl dialect)
        value = _stringify(value)
    elif isinstance(value, (list, tuple)):
        value = _join(value)
    return value

def _format_optdict(optdict, script=False, ignore=None):
    """Formats optdict to a tuple to pass it to tk.call.

    E.g. (script=False):
      {'foreground': 'blue', 'padding': [1, 2, 3, 4]} returns:
      ('-foreground', 'blue', '-padding', '1 2 3 4')"""

    opts = []
    for opt, value in optdict.items():
        if not ignore or opt not in ignore:
            opts.append("-%s" % opt)
            if value is not None:
                opts.append(_format_optvalue(value, script))

    return _flatten(opts)

class CustomNotebook(Notebook):
    """A ttk Notebook with close buttons on each tab"""

    __initialized = False

    def __init__(self, *args, **kwargs):
        if not self.__initialized:
            self.__initialize_custom_style()
            self.__inititialized = True

        kwargs["style"] = "CustomNotebook"
        Notebook.__init__(self, *args, **kwargs)

        self._active = None

        self.bind("<ButtonPress-1>", self.on_close_press, True)
        self.bind("<ButtonRelease-1>", self.on_close_release)
    def on_close_press(self, event):
        """Called when the button is pressed over the close button"""

        element = self.identify(event.x, event.y)

        if "close" in element:
            index = self.index("@%d,%d" % (event.x, event.y))
            self.state(['pressed'])
            self._active = index
            return "break"

    def on_close_release(self, event):
        """Called when the button is released"""
        if not self.instate(['pressed']):
            return

        element =  self.identify(event.x, event.y)
        if "close" not in element:
            # user moved the mouse off of the close button
            return

        index = self.index("@%d,%d" % (event.x, event.y))

        if self._active == index:
            total = self.index('end')
            if total == 2 or index == total - 1:
                return
            if index  == total - 2:
                self.select(total - 3)
            self.forget(index)
            self.event_generate("<<NotebookTabClosed>>")

        self.state(["!pressed"])
        self._active = None
    
    def __initialize_custom_style(self):
        style = Style()
        self.images = (
            PhotoImage("img_close", data='''
                R0lGODlhCAAIAMIBAAAAADs7O4+Pj9nZ2Ts7Ozs7Ozs7Ozs7OyH+EUNyZWF0ZWQg
                d2l0aCBHSU1QACH5BAEKAAQALAAAAAAIAAgAAAMVGDBEA0qNJyGw7AmxmuaZhWEU
                5kEJADs=
                '''),
            PhotoImage("img_closeactive", data='''
                R0lGODlhCAAIAMIEAAAAAP/SAP/bNNnZ2cbGxsbGxsbGxsbGxiH5BAEKAAQALAAA
                AAAIAAgAAAMVGDBEA0qNJyGw7AmxmuaZhWEU5kEJADs=
                '''),
            PhotoImage("img_closepressed", data='''
                R0lGODlhCAAIAMIEAAAAAOUqKv9mZtnZ2Ts7Ozs7Ozs7Ozs7OyH+EUNyZWF0ZWQg
                d2l0aCBHSU1QACH5BAEKAAQALAAAAAAIAAgAAAMVGDBEA0qNJyGw7AmxmuaZhWEU
                5kEJADs=
            ''')
        )
        style.configure('CustomNotebook.Tab', padding=[15, 5])
        
        style.element_create("close", "image", "img_close",
                            ("active", "pressed", "!disabled", "img_closepressed"),
                            ("active", "!disabled", "img_closeactive"), border=8, sticky='')
        style.layout("CustomNotebook", [("CustomNotebook.client", {"sticky": "nswe"})])
        style.layout("CustomNotebook.Tab", [
            ("CustomNotebook.tab", {
                "sticky": "nswe",
                "children": [
                    ("CustomNotebook.padding", {
                        "side": "top",
                        "sticky": "nswe",
                        "children": [
                            ("CustomNotebook.focus", {
                                "side": "top",
                                "sticky": "nswe",
                                "children": [
                                    ("CustomNotebook.label", {"side": "left", "sticky": ''}),
                                    ("CustomNotebook.close", {"side": "left", "sticky": ''}),
                                ]
                        })
                    ]
                })
            ]
        })
    ])

class CustomNotebook2(Notebook):
    """A ttk Notebook with close buttons on each tab"""

    def __init__(self, *args, **kwargs):
        Notebook.__init__(self, *args, **kwargs)

        self._active = None

        self.bind("<ButtonPress-2>", self.on_close_press, True)
        self.bind("<ButtonRelease-2>", self.on_close_release)
    
    def on_close_press(self, event):
        """Called when the button is pressed over the close button"""
        index = self.index("@%d,%d" % (event.x, event.y))
        self.state(['pressed'])
        self._active = index
        return "break"

    def on_close_release(self, event):
        """Called when the button is released"""
        if not self.instate(['pressed']):
            return
        index = self.index("@%d,%d" % (event.x, event.y))
        if self._active == index:
            total = self.index('end')
            if total == 2 or index == total - 1:
                return
            if index  == total - 2:
                self.select(total - 3)
            active_object = self.nametowidget(self.tabs()[index])
            self.forget(index)
            active_object.destroy()
            self.event_generate("<<NotebookTabClosed>>")
        self.state(["!pressed"])
        self._active = None
    

def init_db():
    with SqlliteConn(db_name=db_name) as conn:
        conn.execute('CREATE TABLE IF NOT EXISTS brokers (value TEXT NOT NULL, timestamp TEXT DEFAULT CURRENT_TIMESTAMP, CONSTRAINT brokers_pk PRIMARY KEY (value));')
        conn.execute('CREATE TABLE IF NOT EXISTS topics (value TEXT NOT NULL, timestamp TEXT DEFAULT CURRENT_TIMESTAMP, CONSTRAINT topics_pk PRIMARY KEY (value));')
        conn.execute('CREATE TABLE IF NOT EXISTS files (value TEXT NOT NULL, timestamp TEXT DEFAULT CURRENT_TIMESTAMP, CONSTRAINT files_pk PRIMARY KEY (value));')
        conn.execute('CREATE TABLE IF NOT EXISTS highlighter (idx int NOT NULL, txt TEXT, bg TEXT, fg TEXT, CONSTRAINT highlighter_pk PRIMARY KEY (idx));')
        
def add_tab(tabControl: Notebook, root: Tk):
    tab = Frame(tabControl)
    last_tab = str(tabControl.index('end'))
    tabControl.add(tab, text = get_tab_name(last_tab))
    ConsumerUI(tab, root)
    tab3 = Frame(tabControl)
    b = Button(tab3, command= lambda: add_tab(tabControl, root))
    b.grid(row=0, column=0)
    tabControl.add(tab3, text ='+')
    tabControl.forget(tabControl.select())

def tab_change(*args):
    t_nos=tabControl.index(tabControl.select())
    last_tab = tabControl.index('end') - 1
    if t_nos == last_tab:
        add_tab(tabControl, root)

def get_tab_name(idx: str):
    return idx + ' Consumer'

def on_closing():
        if messagebox.askokcancel("Quit", "Do you want to quit?"):
            root.destroy()
            
init_db()
root = Tk()
root.title('Consumer for Kafka')
root.protocol("WM_DELETE_WINDOW", on_closing)
root.geometry('1050x600')
root.minsize(width=1050, height=600)
customed_style = Style()
customed_style.configure('Custom.TNotebook.Tab', padding=[15, 5])
tabControl = CustomNotebook2(root, style="Custom.TNotebook")
tab1 = Frame(tabControl)
tab2 = Frame(tabControl)

Grid.columnconfigure(root, 0, weight=1)
Grid.rowconfigure(root, 0, weight=1)
last_tab = str(tabControl.index('end') + 1)
tabControl.add(tab1, text = get_tab_name(last_tab))
tabControl.add(tab2, text='+')
tabControl.bind('<<NotebookTabChanged>>', tab_change)
ConsumerUI(tab1, root)
tabControl.grid(row=0, column=0, sticky='nsew')
root.mainloop()
