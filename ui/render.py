import urwid
import pyarrow.parquet as pq

class FileListApp:

    def __init__(self, parquet_file):
        self.parquet_file = parquet_file
        self.file_widgets = []
        self.file_names = []
        self.file_sizes_mb = []
        self.content_types = []

        self.initialize_data()
        self.initialize_ui()

    def format_file_size(self, size_bytes):
        if size_bytes < 1024:
            return f"{size_bytes} bytes"
        elif size_bytes < (1024 ** 2):
            return f"{size_bytes / 1024:.2f} KB"
        elif size_bytes < (1024 ** 3):
            return f"{size_bytes / (1024 ** 2):.2f} MB"
        else:
            return f"{size_bytes / (1024 ** 3):.2f} GB"


    def initialize_data(self):
        # Load data from the Parquet file
        table = pq.read_table(self.parquet_file)

        # Extract the relevant columns
        self.file_names = table['File Name'].to_pylist()
        file_sizes_kb = table['File Size'].to_pylist()
        self.content_types = table['Content Type'].to_pylist()

        # Convert file sizes from KB to MB
        self.file_sizes_mb = [self.format_file_size(size_kb) for size_kb in file_sizes_kb]


    def initialize_ui(self):
        # Create labels for each column
        file_name_label = urwid.Text("File Name")
        file_size_label = urwid.Text("File Size")
        content_type_label = urwid.Text("Content Type")

        # Create columns for column titles
        columns_titles = urwid.Columns([
            ('weight', 1, file_name_label),
            ('weight', 1, file_size_label),
            ('weight', 1, content_type_label),
        ], dividechars=1)

        # Create columns for each file entry
        for file_name, file_size_mb, content_type in zip(self.file_names, self.file_sizes_mb, self.content_types):
            filename = urwid.Text(file_name)
            filesize = urwid.Text(file_size_mb)

            # Handle null content type
            if content_type is None:
                content_type = urwid.Text("No MIME Type Inferred")
            else:
                content_type = urwid.Text(content_type)

            # Create columns for filename, filesize, and content type
            file_columns = urwid.Columns([
                ('weight', 1, filename),
                ('weight', 1, filesize),
                ('weight', 1, content_type),
            ], dividechars=1)

            # Add the file columns to the list of file widgets
            self.file_widgets.append(file_columns)

        # Create a SimpleFocusListWalker to manage list items
        list_walker = urwid.SimpleFocusListWalker(self.file_widgets)

        # Create a ListBox with the initial list walker
        listbox = urwid.ListBox(list_walker)

        # Create a search bar
        search_edit = urwid.Edit("Search: ")

        def update_listbox(new_edit_text):
            list_walker.clear()
            list_walker.append(columns_titles)

            for file_columns in self.file_widgets[1:]:
                filename, filesize, content_type = file_columns.contents
                filename_text = filename[0].text.lower()
                filesize_text = filesize[0].text.lower()
                content_type_text = content_type[0].text.lower()
                search_text = new_edit_text.lower()

                if search_text in filename_text or search_text in filesize_text or search_text in content_type_text:
                    list_walker.append(file_columns)

        def on_change(widget, newtext):
            update_listbox(newtext)

        def on_exit(key):
            if key == 'esc':
                raise urwid.ExitMainLoop()

        urwid.connect_signal(search_edit, 'change', on_change)

        frame = urwid.Frame(listbox)
        search_line = urwid.LineBox(urwid.AttrMap(urwid.Filler(search_edit, valign='top'), 'search_bar'))
        frame_line = urwid.LineBox(frame)

        body = urwid.Pile([
            ('weight', 0.5, search_line),
            ('weight', 9, frame_line),
        ])

        self.loop = urwid.MainLoop(body, unhandled_input=on_exit)



    def run(self):
        self.loop.run()

