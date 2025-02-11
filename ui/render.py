import pyarrow.parquet as pq
from urwid import (
    Text,
    Columns,
    ListBox,
    Edit,
    SimpleFocusListWalker,
    Frame,
    LineBox,
    AttrMap,
    Filler,
    Pile,
    MainLoop,
    ExitMainLoop,
    connect_signal,
)

class App:
    def __init__(self, parquet_file):
        self.parquet_file = parquet_file
        self.file_widgets = []

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

    def initialize_ui(self):
        # Load data from the Parquet file
        table = pq.read_table(self.parquet_file)

        # Extract the relevant columns from the Parquet table
        file_names = table['File Name'].to_pylist()
        file_sizes_kb = table['File Size'].to_pylist()
        content_types = table['Content Type'].to_pylist()

        # Format file sizes
        file_sizes_mb = [self.format_file_size(size_kb) for size_kb in file_sizes_kb]

        # Create labels for each column
        file_name_label = Text("File Name")
        file_size_label = Text("File Size")
        content_type_label = Text("Content Type")

        # Create columns for column titles
        columns_titles = Columns([
            ('weight', 1, file_name_label),
            ('weight', 1, file_size_label),
            ('weight', 1, content_type_label),
        ], dividechars=1)
        self.file_widgets.append(columns_titles)

        # Create columns for each file entry
        for file_name, file_size_mb, content_type in zip(file_names, file_sizes_mb, content_types):
            filename = Text(file_name)
            filesize = Text(file_size_mb)  # Use converted MB size

            # Handle null content type
            if content_type is None:
                content_type = Text("No MIME Type Inferred")
            else:
                content_type = Text(content_type)

            # Create columns for filename, filesize, and content type
            file_columns = Columns([
                ('weight', 1, filename),
                ('weight', 1, filesize),
                ('weight', 1, content_type),
            ], dividechars=1)

            # Add the file columns to the list of file widgets
            self.file_widgets.append(file_columns)

        # Create a SimpleFocusListWalker to manage list items
        list_walker = SimpleFocusListWalker(self.file_widgets)

        # Create a ListBox with the initial list walker
        listbox = ListBox(list_walker)

        # Create a search bar
        search_edit = Edit("Search: ")

        # Function to update the listbox based on search text
        def update_listbox(new_edit_text):
            list_walker.clear()

            # Add the column titles
            list_walker.append(columns_titles)

            # Filter and add file entries matching the search text
            for file_columns in self.file_widgets[1:]:
                filename, filesize, content_type = file_columns.contents
                filename_text = filename[0].text.lower()
                filesize_text = filesize[0].text.lower()
                content_type_text = content_type[0].text.lower()
                search_text = new_edit_text.lower()

                # Check if search_text is in any of the three elements using an "OR" condition
                if search_text in filename_text or search_text in filesize_text or search_text in content_type_text:
                    list_walker.append(file_columns)

        # Function to handle Enter key press
        def on_change(widget, newtext):
            update_listbox(newtext)
        
        # Handles exit
        def on_exit(key):
            if key == 'esc':
                raise ExitMainLoop()

        # Connect the change event of the search bar to the update function
        connect_signal(search_edit, 'change', on_change)

        # Create a Frame to add a header
        frame = Frame(listbox)

        search_line = LineBox(AttrMap(Filler(search_edit, valign='top'), 'search_bar'))

        # Create a LineBox for the frame that takes up 90% of the screen
        frame_line = LineBox(frame)

        body = Pile([
            ('weight', 0.5, search_line),
            ('weight', 9, frame_line),  # Adjust the weights as needed (10% and 90%)
        ])

        self.loop = MainLoop(body, unhandled_input=on_exit)

    def run(self):
        self.loop.run()