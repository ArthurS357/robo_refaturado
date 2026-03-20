import tkinter as tk
from tkinter import ttk


class BaseTab:
    def __init__(self, parent: ttk.Frame, app):
        self.parent = parent
        self.app = (
            app  # Referência ao AuditApp principal (para acessar motor, config, etc)
        )

    def _criar_area_rolavel(self, parent_frame):
        """
        Cria um container com barra de rolagem vertical.
        Lida com conflitos de scroll e redimensionamento.
        """
        outer_frame = ttk.Frame(parent_frame)
        outer_frame.pack(fill="both", expand=True)

        canvas = tk.Canvas(outer_frame, highlightthickness=0)
        scrollbar = ttk.Scrollbar(outer_frame, orient="vertical", command=canvas.yview)

        inner_frame = ttk.Frame(canvas)

        inner_frame.bind(
            "<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )

        canvas_window = canvas.create_window((0, 0), window=inner_frame, anchor="nw")

        def _configure_canvas_width(event):
            if inner_frame.winfo_reqwidth() != event.width:
                canvas.itemconfig(canvas_window, width=event.width)

        canvas.bind("<Configure>", _configure_canvas_width)
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        def _on_mousewheel(event):
            widget_under_mouse = event.widget
            scrollable_widgets = ("Treeview", "Text", "Listbox", "TCombobox")

            try:
                widget_class = widget_under_mouse.winfo_class()
                if widget_class in scrollable_widgets:
                    return
            except Exception:
                pass

            if canvas.bbox("all")[3] > canvas.winfo_height():
                if hasattr(event, "delta") and event.delta:
                    canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
                elif hasattr(event, "num"):
                    if event.num == 4:
                        canvas.yview_scroll(-1, "units")
                    if event.num == 5:
                        canvas.yview_scroll(1, "units")

        outer_frame.bind(
            "<Enter>", lambda e: canvas.bind_all("<MouseWheel>", _on_mousewheel)
        )
        outer_frame.bind(
            "<Enter>", lambda e: canvas.bind_all("<Button-4>", _on_mousewheel)
        )
        outer_frame.bind(
            "<Enter>", lambda e: canvas.bind_all("<Button-5>", _on_mousewheel)
        )

        outer_frame.bind("<Leave>", lambda e: canvas.unbind_all("<MouseWheel>"))
        outer_frame.bind("<Leave>", lambda e: canvas.unbind_all("<Button-4>"))
        outer_frame.bind("<Leave>", lambda e: canvas.unbind_all("<Button-5>"))

        return inner_frame
