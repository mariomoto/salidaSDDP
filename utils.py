import json
import os
import tkinter as tk
from tkinter.filedialog import askdirectory
from tkinter import ttk

HISTORY_FILE = os.path.expanduser("~/.folder_chooser_history.json")
MAX_HISTORY = 10


def load_history():
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, "r") as f:
            return json.load(f)
    return []


def save_history(history):
    with open(HISTORY_FILE, "w") as f:
        json.dump(history, f, indent=2)


def add_to_history(history, path):
    if path in history:
        history.remove(path)      # Move to top if already exists
    history.insert(0, path)
    return history[:MAX_HISTORY]  # Keep only the most recent entries


def choose_directory_with_history():
    history = load_history()

    # If there's no history, just open the native dialog directly
    if not history:
        root = tk.Tk()
        root.withdraw()
        directory = askdirectory()
        root.destroy()
        if directory:
            save_history(add_to_history([], directory))
        return directory

    # Build a small history picker window
    selected = {"path": None}

    root = tk.Tk()
    root.title("Choose Folder")
    root.resizable(False, False)

    tk.Label(root, text="Recent folders:", font=("Helvetica", 11, "bold")).pack(
        anchor="w", padx=12, pady=(12, 4)
    )

    frame = tk.Frame(root)
    frame.pack(fill="both", expand=True, padx=12)

    listbox = tk.Listbox(frame, width=100, height=min(len(history), 8), selectmode="single")
    listbox.pack(side="left", fill="both", expand=True)

    scrollbar = ttk.Scrollbar(frame, orient="vertical", command=listbox.yview)
    scrollbar.pack(side="right", fill="y")
    listbox.configure(yscrollcommand=scrollbar.set)

    for path in history:
        listbox.insert(tk.END, path)

    def use_selected():
        idx = listbox.curselection()
        if idx:
            selected["path"] = history[idx[0]]
            root.destroy()

    def browse_new():
        selected: dict[str, str | None] = {"path": None}
        root.withdraw()
        path = askdirectory()
        root.destroy()
        selected["path"] = path if path else None

    btn_frame = tk.Frame(root)
    btn_frame.pack(fill="x", padx=12, pady=10)

    tk.Button(btn_frame, text="Use Selected", command=use_selected, width=14).pack(
        side="left", padx=(0, 6)
    )
    tk.Button(btn_frame, text="Browse New…", command=browse_new, width=14).pack(side="left")
    tk.Button(btn_frame, text="Cancel", command=root.destroy, width=10).pack(side="right")

    # Double-click selects immediately
    listbox.bind("<Double-Button-1>", lambda e: use_selected())

    root.mainloop()

    directory = selected["path"]
    if directory:
        save_history(add_to_history(history, directory))
    return directory


# Usage
if __name__ == "__main__":
    directory = choose_directory_with_history()
    print("Chosen directory:", directory)