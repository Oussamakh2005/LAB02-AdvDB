import tkinter as tk
from tkinter import ttk, messagebox
from sql import execute_query


# --------------------------------------------------------------
# YOUR SQL INTERPRETER
# Replace this with your real interpreter.
# Must return: list[dict]
# --------------------------------------------------------------
def run_query(sql: str):
    results = execute_query(sql,"schema.json")  
    return results


# --------------------------------------------------------------
# TKINTER UI
# --------------------------------------------------------------
class SQLInterface:
    def __init__(self, root):
        self.root = root
        self.root.title("SQL Interpreter UI")
        self.root.geometry("700x500")

        # ------------------- SQL Input Area -------------------
        input_frame = tk.Frame(root, padx=10, pady=10)
        input_frame.pack(fill="x")

        tk.Label(input_frame, text="SQL Query:").pack(anchor="w")
        self.text_sql = tk.Text(input_frame, height=4)
        self.text_sql.pack(fill="x")

        btn = tk.Button(input_frame, text="Execute", command=self.on_execute,
                        bg="#0078D7", fg="white", font=("Arial", 11, "bold"))
        btn.pack(pady=10)

        # ------------------- Result Table -------------------
        table_frame = tk.Frame(root)
        table_frame.pack(fill="both", expand=True)

        self.tree = ttk.Treeview(table_frame, show="headings")
        self.tree.pack(fill="both", expand=True)

        # Add scrollbar
        scrollbar = tk.Scrollbar(table_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscroll=scrollbar.set)
        scrollbar.pack(side="right", fill="y")

    # ----------------------------------------------------------
    # When clicking "Execute"
    # ----------------------------------------------------------
    def on_execute(self):
        sql = self.text_sql.get("1.0", tk.END).strip()

        if not sql:
            messagebox.showwarning("Warning", "Please enter a SQL query.")
            return

        try:
            results = run_query(sql)  # <-- your interpreter

            if not results:
                messagebox.showinfo("Info", "Query executed. No results.")
                self.clear_table()
                return
            
            if not isinstance(results, list) or not isinstance(results[0], dict):
                raise ValueError("Interpreter must return list[dict].")
            
            self.populate_table(results)

        except Exception as e:
            messagebox.showerror("Error", str(e))


    # ----------------------------------------------------------
    # Clears the table
    # ----------------------------------------------------------
    def clear_table(self):
        for col in self.tree["columns"]:
            self.tree.heading(col, text="")
        self.tree.delete(*self.tree.get_children())


    # ----------------------------------------------------------
    # Fill table with list-of-dict results
    # ----------------------------------------------------------
    def populate_table(self, data: list[dict]):
        self.clear_table()

        # Extract column names from dict keys
        columns = list(data[0].keys())
        self.tree["columns"] = columns

        for col in columns:
            self.tree.heading(col, text=col)
            self.tree.column(col, width=120)

        # Insert rows
        for row in data:
            values = [row.get(col, "") for col in columns]
            self.tree.insert("", tk.END, values=values)



# --------------------------------------------------------------
# Run the App
# --------------------------------------------------------------
if __name__ == "__main__":
    root = tk.Tk()
    app = SQLInterface(root)
    root.mainloop()
