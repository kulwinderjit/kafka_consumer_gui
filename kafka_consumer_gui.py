from tkinter import filedialog
from tkinter import messagebox
from tkinter import *
from tkinter.ttk import *
from kafka import KafkaConsumer
from datetime import datetime
from kafka.errors import NoBrokersAvailable, KafkaTimeoutError
from typing import List, Dict
import sqlite3
from tkinter import colorchooser
import json
import os
db_name = 'consumer_config.db'
highligher_rules: List[Dict] = []
version = "1"

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

def append_message(key, message):
    format = json_format_filter_var.get()
    line = None
    if format == 1:
        try:
            message = json.loads(message)
            line = str.format('{} : {}{}{}', key, '\n', json.dumps(message, indent=2), '\n')
        except:
            line = str.format('{} : {}{}', key, message, '\n')
    else:
        line = str.format('{} : {}{}', key, message, '\n')
    tag = get_tag_name(line)
    if tag:
        value.insert(END, line, tag)
    else:
        value.insert(END, line)
    if scroll_lock_button_var.get() == 0:
        value.see(END)
def get_tag_name(line: str):
    for rule in highligher_rules:
        if rule['txt'] in line:
            return rule['idx']
    return None
def consume(consumer):
    filter = str(message_filter.get())
    or_filters = filter.split(',')
    or_filters = [f for f in or_filters if len(f) > 0]
    and_filters = filter.split(';')
    and_filters = [f for f in and_filters if len(f) > 0]
    for msg in consumer:
        if msg.key is not None:
            key = msg.key.decode()
        else:
            key = "null"
        message = msg.value.decode()
        if len(and_filters) > 1:
            if all(f in message or f in key for f in and_filters):
                append_message(key, message)
        elif len(or_filters) > 0:
            if any(f in message or f in key for f in or_filters):
                append_message(key, message)
        else:
            append_message(key, message)
    main_window.after(100, func=lambda: consume(consumer))

def write_to_file(csf, key, message):
    format = json_format_filter_var.get()
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

def consume_to_file(consumer, file):
    value.insert(END, 'Started consuming to file : ' + str(datetime.now()) + '\n')
    filter = str(message_filter.get())
    or_filters = filter.split(',')
    or_filters = [f for f in or_filters if len(f) > 0]
    and_filters = filter.split(';')
    and_filters = [f for f in and_filters if len(f) > 0]
    with open(file,'w',newline='') as csf:
        for msg in consumer:
            if msg.key is not None:
                key = msg.key.decode()
            else:
                key = "null"
            message = msg.value.decode()
            if len(and_filters) > 1:
                if all(f in message or f in key for f in and_filters):
                    write_to_file(csf, key, message)
            elif len(or_filters) > 0:
                if any(f in message or f in key for f in or_filters):
                    write_to_file(csf, key, message)
            else:
                write_to_file(csf, key, message)
            main_window.update()
    value.insert(END, 'Consume to file completed: ' + str(datetime.now()) + '\n')
    value.see(END)

consumer_after_id = None
consumer = None

def get_kafka_consumer(servers, type, timeout):
    consumer = KafkaConsumer(bootstrap_servers=servers,auto_offset_reset=type, consumer_timeout_ms=timeout)
    return consumer

def consume_to_kafka(servers, topics, type, file):
    topis_list = str(topics).split(',')
    try:
        global consumer_after_id
        global consumer
        if consumer is not None:
            consumer.close()
            consume_button.config(text='Consume')
            consumer = None
            return
        if type == 'latest':
            main_window.update()
            consumer = get_kafka_consumer(servers, type, 100)
            main_window.update()
            consumer.subscribe(topis_list)
            main_window.update()
            if consumer_after_id is not None:
                main_window.after_cancel(consumer_after_id)
            consumer_after_id = main_window.after(100, func=lambda: consume(consumer))
            value.insert(END, 'Started consuming ' + topics + ' : '+ str(datetime.now()) + '\n')
            value.see(END)
            consume_button.config(text='Stop Consuming')
        else:
            main_window.update()
            valid, file = validate_file_path(file)
            if not valid:
                messagebox.showwarning(title='Select file first', message=file)
                return
            consumer = get_kafka_consumer(servers, type, 1000)
            main_window.update()
            consumer.subscribe(topis_list)
            main_window.update()
            consume_button.config(text='Stop Consuming')
            main_window.update()
            consume_to_file(consumer, file)
            consume_button.config(text='Consume')
            main_window.update()
            if consumer is not None:
                consumer.close()
            consumer = None
    except NoBrokersAvailable as e:
        value.insert(END, str(datetime.now()) + ': ' + str(e) + '\n')
        value.see(END)
    except KafkaTimeoutError as e:
        value.insert(END, str(datetime.now()) + ': ' + str(e) + '\n')
        value.see(END)
    except ValueError as e:
        value.insert(END, str(datetime.now()) + ': ' + str(e) + '\n')
        value.see(END)

def validate_file_path(file):
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

def init_db():
    with SqlliteConn(db_name=db_name) as conn:
        conn.execute('CREATE TABLE IF NOT EXISTS brokers (value TEXT NOT NULL, timestamp TEXT DEFAULT CURRENT_TIMESTAMP, CONSTRAINT brokers_pk PRIMARY KEY (value));')
        conn.execute('CREATE TABLE IF NOT EXISTS topics (value TEXT NOT NULL, timestamp TEXT DEFAULT CURRENT_TIMESTAMP, CONSTRAINT topics_pk PRIMARY KEY (value));')
        conn.execute('CREATE TABLE IF NOT EXISTS files (value TEXT NOT NULL, timestamp TEXT DEFAULT CURRENT_TIMESTAMP, CONSTRAINT files_pk PRIMARY KEY (value));')
        conn.execute('CREATE TABLE IF NOT EXISTS highlighter (idx int NOT NULL, txt TEXT, bg TEXT, fg TEXT, CONSTRAINT highlighter_pk PRIMARY KEY (idx));')

def get_highlighter():
    l = []
    with SqlliteConn(db_name=db_name) as conn:
        cursor = conn.execute('select idx, txt, bg, fg from highlighter order by idx asc')
        for r in cursor:
            l.append({'idx': str(r[0]), 'txt':r[1], 'bg': r[2], 'fg': r[3]})
    return l
def set_highlighter(filters: List[Dict]):
    with SqlliteConn(db_name=db_name) as conn:
        conn.execute('delete from highlighter')
    with SqlliteConn(db_name=db_name) as conn:
        idx: int = 0
        for filter in filters:
            conn.execute('insert into highlighter(idx, txt, bg, fg) values(?,?,?,?)', (idx, filter['txt'], filter['bg'], filter['fg']))
            idx += 1
def get_list(type):
    l = []
    with SqlliteConn(db_name=db_name) as conn:
        cursor = conn.execute('select value from %s order by timestamp desc' % type)
        for r in cursor:
            l.append(r[0])
    return l

def relenght_tables():
    maxlen = 20
    with SqlliteConn(db_name=db_name) as conn:
        conn.execute('delete from brokers where timestamp in (select b.timestamp from brokers b left join (select timestamp from brokers order by timestamp desc limit ' + str(maxlen) + ') b1 on b1.timestamp = b.timestamp where b1.timestamp is null);')
        conn.execute('delete from topics where timestamp in (select b.timestamp from topics b left join (select timestamp from topics order by timestamp desc limit ' + str(maxlen) + ') b1 on b1.timestamp = b.timestamp where b1.timestamp is null);')
        conn.execute('delete from files where timestamp in (select b.timestamp from files b left join (select timestamp from files order by timestamp desc limit ' + str(maxlen) + ') b1 on b1.timestamp = b.timestamp where b1.timestamp is null);')

def start_consumer():
    _broker = str(servers.get()).strip()
    _topic = str(topic.get()).strip()
    _file = str(file.get()).strip()
    _type = str(type.get()).strip()
    if len(_broker) == 0 or len(_topic) == 0:
        messagebox.showwarning(title='Fields required',message='Broker and topic fields are required')
        return
    with SqlliteConn(db_name=db_name) as conn:
        conn.execute('insert or replace into brokers(value) values(?)', (_broker,))
        conn.execute('insert or replace into topics(value) values(?)', (_topic,))
        if len(_file) > 0:
            conn.execute('insert or replace into files(value) values(?)', (_file,))
    relenght_tables()
    update_lists()
    consume_to_kafka(_broker, _topic, _type, _file)

def get_latest_msg():
    with SqlliteConn(db_name=db_name) as conn:
        cursor = conn.execute('select value,timestamp from messages order by timestamp desc limit 1')
        for r in cursor:
            return (r[0], r[1])
    return (None,None)

def update_lists():
    servers['values'] = get_list('brokers')
    if len(servers['values'])>0:
        servers.current(0)
    topic['values'] = get_list('topics')
    if len(topic['values'])>0:
        topic.current(0)
    file['values'] = get_list('files')
    if len(file['values'])>0:
        file.current(0)

def sysopen(file_box: Combobox):
    filetxt = file_box.get()
    if len(str.lstrip(filetxt))>0:
        os.startfile(filetxt, 'open')

def browse(file_box):
    filename = filedialog.asksaveasfilename(confirmoverwrite=True)
    if len(filename) == 0:
        return
    values = file['values']
    if len(values) ==0:
        file['values'] = (filename)
    else:
        file['values'] = (filename, ) + values
    file.current(0)

def on_closing():
    if messagebox.askokcancel("Quit", "Do you want to quit?"):
        global consumer
        if consumer is not None:
            consumer.close()
        main_window.destroy()
def show_about():
    d = About(main_window)
    main_window.wait_window(d.tp)
def configure_highlight():
    d = Highlighter(main_window, highligher_rules)
    main_window.wait_window(d.tp)
    if d.ok_done:
        set_highlighter(highligher_rules)
        for rule in highligher_rules:
            value.tag_configure(rule['idx'], background=rule['bg'], foreground=rule['fg'])
def initiate_highlight():
    _highligher_rules = get_highlighter()
    for rule in _highligher_rules:
        highligher_rules.append(rule)
        value.tag_configure(rule['idx'], background=rule['bg'], foreground=rule['fg'])
init_db()
main_window = Tk()
main_window.title('Consumer for Kafka')
main_window.protocol("WM_DELETE_WINDOW", on_closing)
buttons_frame = Frame(main_window)
about_button = Button(buttons_frame, text='About', command=show_about)
about_button.grid(row=0, column=3, sticky=N+E, padx=5, pady=5)
servers_label = LabelFrame(buttons_frame, text='Bootstrap servers (comma separated)', padding='2 2 2 2')
servers = Combobox(servers_label)
servers.grid(row=0, column=0, sticky=N+W+E)
servers_label.grid(row=0, column=0, padx=5, sticky=N+W+E)
servers_label.columnconfigure(0, weight=1)

topic_label = LabelFrame(buttons_frame, text='Topic (comma separated)', padding='2 2 2 2')
topic = Combobox(topic_label)
topic.grid(row=0, column=0, sticky=N+W+E)
topic_label.grid(row=0, column=1, padx=5, sticky=N+W+E)
topic_label.columnconfigure(0, weight=1)

message_filter_label = LabelFrame(buttons_frame, text='Message filters (, for OR ; for AND)', padding='2 2 2 2')
message_filter = Entry(message_filter_label, textvariable=StringVar())
message_filter.grid(row=0, column=0, sticky=S+W+E)
message_filter_label.grid(row=2, column=0, padx=5, sticky=S+W+E)
message_filter_label.columnconfigure(0, weight=1)

file_label = LabelFrame(buttons_frame, text='File', padding='2 2 2 2')
file = Combobox(file_label, state='readonly')
file.grid(row=0, column=0, sticky=N+W+E)
file_label.grid(row=1, column=1, padx=5, sticky=N+W+E)
file_label.columnconfigure(0, weight=1)

file_buttons_frame = Frame(buttons_frame)
browse_button = Button(file_buttons_frame, text='Browse..', command= lambda: browse(file))
browse_button.grid(row=0, column=1, sticky=S+W)
open_button = Button(file_buttons_frame, text='Open', command= lambda: sysopen(file))
open_button.grid(row=0, column=2, sticky=S+W)
file_buttons_frame.grid(row=1, column=2, sticky=S+W)

type_label = LabelFrame(buttons_frame, text='Fetch type', padding='2 2 2 2')
type = Combobox(type_label, state='readonly')
type.grid(row=0, column=0, sticky=N+W+E)
type_label.grid(row=1, column=0, padx=5, sticky=N+W+E)
type_label.columnconfigure(0, weight=1)
type['values'] = ('latest', 'earliest')
type.current(0)


control_buttons_frame = Frame(buttons_frame)
highlight_button  = Button(control_buttons_frame, text='Highlight', command=configure_highlight)
highlight_button.grid(row=0, column=0, padx=5)
consume_button = Button(control_buttons_frame, text='Consume', command= lambda: start_consumer())
consume_button.grid(row=0, column=1, padx=5, sticky=S+W)
scroll_lock_button_var = IntVar()
scroll_lock_button = Checkbutton(control_buttons_frame, text='ScrollLock', variable=scroll_lock_button_var)
scroll_lock_button.grid(row=0, column=4, sticky=W+S, padx=5)
clr_button = Button(control_buttons_frame, text='Clear', command = lambda: value.delete('1.0', END))
clr_button.grid(row=0, column=2, sticky=W+S, padx=5)
json_format_filter_var = IntVar()
json_format_filter = Checkbutton(control_buttons_frame, text='Format Json', variable=json_format_filter_var)
json_format_filter.grid(row=0, column=3, sticky=S+W)
control_buttons_frame.grid(row=2, column=1, sticky=S+W+E)

buttons_frame.grid(row=0, column=0, sticky=N+W+E)
buttons_frame.columnconfigure(0, weight=1)
buttons_frame.columnconfigure(1, weight=1)
buttons_frame.columnconfigure(2, weight=1)
buttons_frame.columnconfigure(3, weight=1)

value_label = LabelFrame(main_window, text='Output', padding='2 2 2 2')
value = Text(value_label, wrap=NONE, undo=True, maxundo=-1, autoseparators=True)
scrollb = Scrollbar(value_label, command=value.yview)
scrollb_h = Scrollbar(value_label, command=value.xview, orient=HORIZONTAL)
value['yscrollcommand'] = scrollb.set
value['xscrollcommand'] = scrollb_h.set
value.grid(row=0, column=0, padx=2, pady=2, sticky=N+W+E+S)
value_label.grid(row=1, column=0, padx=5, sticky=N+W+E+S)
value_label.columnconfigure(0, weight=1)
value_label.rowconfigure(0, weight=1)
scrollb.grid(row=0, column=1, sticky=N+S)
scrollb_h.grid(row=1, column=0, sticky=E+W)

main_window.rowconfigure(0, weight=1)
main_window.rowconfigure(1, weight=16)
main_window.columnconfigure(0, weight=1)
main_window.style = Style()
main_window.geometry('900x600')
main_window.minsize(width=900, height=600)
update_lists()
main_window.style.theme_use("vista")
initiate_highlight()
main_window.mainloop()
