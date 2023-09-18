import urwid
import pyarrow.parquet as pq

# Load data from your file_index.parquet file
parquet_file = "file_index.parquet"
table = pq.read_table(parquet_file)

# Extract the relevant columns from the Parquet table
file_names = table['File Name'].to_pylist()
file_sizes_kb = table['File Size'].to_pylist()
content_types = table['Content Type'].to_pylist()

# Convert file sizes from KB to MB
file_sizes_mb = [f"{float(size_kb) / 1024:.2f} MB" for size_kb in file_sizes_kb]

# Create labels for each column
file_name_label = urwid.Text("File Name")
file_size_label = urwid.Text("File Size (MB)")  # Updated label
content_type_label = urwid.Text("Content Type")

# Create a list of widgets for each file entry
file_widgets = []

# Create columns for column titles
columns_titles = urwid.Columns([
    ('weight', 1, file_name_label),
    ('weight', 1, file_size_label),
    ('weight', 1, content_type_label),
], dividechars=1)
file_widgets.append(columns_titles)

# Create columns for each file entry
for file_name, file_size_mb, content_type in zip(file_names, file_sizes_mb, content_types):
    filename = urwid.Text(file_name)
    filesize = urwid.Text(file_size_mb)  # Use converted MB size

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
    file_widgets.append(file_columns)

# Create a SimpleFocusListWalker to manage list items
list_walker = urwid.SimpleFocusListWalker(file_widgets)

# Create a ListBox with the initial list walker
listbox = urwid.ListBox(list_walker)

# Create a search bar
search_edit = urwid.Edit("Search: ")

def mouse_event(self, size, event, button, col, row, focus):
    pass

# Function to update the listbox based on search text
def update_listbox(new_edit_text):
    # Clear the list walker
    list_walker.clear()

    # Add the column titles
    list_walker.append(columns_titles)

    # Filter and add file entries matching the search text
    for file_columns in file_widgets[1:]:
        filename, filesize, content_type = file_columns.contents
        filename_text = filename[0].text.lower()
        search_text = new_edit_text.lower()

        if search_text in filename_text:
            list_walker.append(file_columns)

debug = urwid.Text('')

# Function to handle Enter key press
def on_enter(widget, newtext):
    # print(widget, key)
    search_text = search_edit.get_edit_text()
    # print(search_text)
    update_listbox(search_text)
    debug.set_text("Edit widget changed to %s" % newtext)
    pass

# Connect the change event of the search bar to the update function
urwid.connect_signal(search_edit, 'change', on_enter)

# Create a Frame to add a header (optional)
frame = urwid.Frame(listbox)

body = urwid.Pile([
    # urwid.Filler(debug),
    urwid.LineBox(urwid.AttrMap(urwid.Filler(search_edit, valign='bottom'), 'search_bar')),
    urwid.LineBox(frame),
])

loop = urwid.MainLoop(body)
loop.run()
