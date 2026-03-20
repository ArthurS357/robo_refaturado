import sys
import traceback
import tkinter as tk
from tkinter import ttk, messagebox
import threading
import os
import glob
import ctypes

try:
    import pandas as pd  # type: ignore[reportUnusedImport]

    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

from tab_execucao import TabExecucao
from tab_historico import TabHistorico
from tab_ferramentas import TabFerramentas
from tab_configuracao import TabConfiguracao

try:
    from audit_core import AuditMotor, GerenciadorLog
    from audit_data import DataProcessor
except ImportError as e:

    class AuditMotor:
        def __init__(self):
            self.pasta_downloads = "."
            self.rodando_event = threading.Event()
            self.pausado_event = threading.Event()

        def carregar_config(self):
            return {}

    class GerenciadorLog:
        def importar_historico(self, c):
            return 0

    class DataProcessor:
        pass

    INIT_ERROR = str(e)
else:
    INIT_ERROR = None


class AuditApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Robô Audit | Asset Management System")
        self.geometry("1280x850")
        self.minsize(1024, 768)

        if INIT_ERROR:
            messagebox.showerror(
                "Erro de Dependência", f"Falha ao carregar módulos:\n{INIT_ERROR}"
            )

        try:
            self.motor = AuditMotor()
            self.logger = GerenciadorLog()
            self.config = self.motor.carregar_config()
            self.data_processor = getattr(self.motor, "data_processor", DataProcessor())
        except Exception as e:
            print(f"Erro ao inicializar motor: {e}")
            self.config = {}

        self.carregar_memoria_historico()

        self.dark_mode = tk.BooleanVar(value=self.config.get("dark_mode", False))
        self.path_rede = tk.StringVar(value=self.config.get("path", ""))
        self.limpar_apos_mover = tk.BooleanVar(
            value=self.config.get("clean_after", False)
        )
        self.tempo_persistencia = tk.IntVar(value=self.config.get("timeout_dl", 200))
        self.tempo_persist_btn = tk.IntVar(
            value=self.config.get("timeout_retry_btn", 300)
        )
        self.xpath_wait = tk.StringVar(value=self.config.get("xpath_wait", ""))

        self.coords = {}
        self.botoes_nomes = [
            ("🖱️ Botão Direito", "btn_rclick"),
            ("📤 Botão Exportar", "btn_exp"),
            ("📄 Opção CSV", "btn_csv"),
            ("⬇️ Botão Download", "btn_dl"),
        ]
        for _, k in self.botoes_nomes:
            self.coords[k] = tk.StringVar(
                value=self.config.get("coords", {}).get(k, "0,0")
            )

        # ORDEM CORRETA: Estilos -> Cores -> Layout -> Re-aplicar Cores (Evita o erro nas Abas)
        self._setup_style()
        self.aplicar_tema()
        self._build_layout()
        self.aplicar_tema()

        self.protocol("WM_DELETE_WINDOW", self.fechar_aplicacao)

        self.log("Sistema pronto. Engine V11 carregada.")
        if not HAS_PANDAS:
            self.log("Aviso: Biblioteca Pandas não encontrada.")

    def fechar_aplicacao(self):
        if hasattr(self, "motor") and self.motor.rodando_event.is_set():
            if not messagebox.askyesno(
                "Aviso",
                "Há uma execução em andamento.\nFechar abortará o processo.\nDeseja sair?",
            ):
                return
            if hasattr(self, "tab_exec"):
                self.tab_exec.parar()
        self.destroy()

    def carregar_memoria_historico(self):
        try:
            path_dl = str(self.motor.pasta_downloads)
            if not os.path.exists(path_dl):
                return
            lista = glob.glob(os.path.join(path_dl, "Relatorio_Execucao_*.csv"))
            if lista:
                ultimo = max(lista, key=os.path.getctime)
                self.logger.importar_historico(ultimo)
        except Exception as e:
            pass

    def _setup_style(self):
        self.style = ttk.Style()
        self.style.theme_use("clam")
        self.font_h1 = ("Segoe UI", 18, "bold")
        self.font_h2 = ("Segoe UI", 12, "bold")
        self.font_body = ("Segoe UI", 10)
        self.font_small = ("Segoe UI", 9)
        self.font_mono = ("Consolas", 10)

        self.style.configure("TNotebook", tabposition="n")
        self.style.configure("TNotebook.Tab", font=self.font_body, padding=[20, 8])
        self.style.configure(
            "Treeview", rowheight=28, borderwidth=0, font=self.font_small
        )
        self.style.configure(
            "Treeview.Heading", font=("Segoe UI", 10, "bold"), borderwidth=1
        )
        self.style.layout("Treeview", [("Treeview.treearea", {"sticky": "nswe"})])

    def aplicar_tema(self):
        # CORES OFICIAIS SANTANDER
        santander_red = "#EC0000"
        santander_dark_red = "#CC0000"

        if self.dark_mode.get():
            colors = {
                "bg": "#202020",
                "panel": "#2D2D2D",
                "fg": "#E0E0E0",
                "fg_dim": "#A0A0A0",
                "accent": santander_red,
                "success": "#4CAF50",
                "warning": "#FFC107",
                "danger": "#F44336",
                "select": "#404040",
                "border": "#505050",
                "input": "#181818",
            }
        else:
            colors = {
                "bg": "#F4F4F4",
                "panel": "#FFFFFF",
                "fg": "#333333",
                "fg_dim": "#666666",
                "accent": santander_red,
                "success": "#2E7D32",
                "warning": "#F57C00",
                "danger": "#D32F2F",
                "select": "#FFEBEE",
                "border": "#CCCCCC",
                "input": "#FFFFFF",
            }

        self.colors = colors
        self.configure(bg=colors["bg"])

        s = self.style
        s.configure("TFrame", background=colors["bg"])
        s.configure(
            "TLabel",
            background=colors["bg"],
            foreground=colors["fg"],
            font=self.font_body,
        )

        s.configure("Card.TFrame", background=colors["panel"])
        s.configure(
            "Card.TLabelframe",
            background=colors["panel"],
            foreground=colors["fg"],
            bordercolor=colors["border"],
        )
        s.configure(
            "Card.TLabelframe.Label",
            background=colors["panel"],
            foreground=colors["accent"],
            font=self.font_h2,
        )
        s.configure("Card.TLabel", background=colors["panel"], foreground=colors["fg"])

        s.configure(
            "ToolCard.TFrame", background=colors["panel"], relief="flat", borderwidth=1
        )
        s.configure(
            "ToolCard.TLabel", background=colors["panel"], foreground=colors["fg"]
        )
        s.configure(
            "ToolTitle.TLabel",
            background=colors["panel"],
            foreground=colors["accent"],
            font=self.font_h2,
        )
        s.configure(
            "ToolIcon.TLabel", background=colors["panel"], font=("Segoe UI Emoji", 24)
        )

        s.configure("TNotebook", background=colors["bg"])
        s.configure(
            "TNotebook.Tab", background=colors["bg"], foreground=colors["fg_dim"]
        )
        s.map(
            "TNotebook.Tab",
            background=[("selected", colors["panel"])],
            foreground=[("selected", colors["accent"])],
        )

        s.configure(
            "Treeview",
            background=colors["input"],
            fieldbackground=colors["input"],
            foreground=colors["fg"],
        )
        s.configure(
            "Treeview.Heading",
            background=colors["panel"],
            foreground=colors["fg"],
            relief="flat",
        )
        s.map(
            "Treeview",
            background=[("selected", colors["select"])],
            foreground=[("selected", santander_dark_red)],
        )

        if self.dark_mode.get():
            s.configure(
                "Treeview",
                background="#333333",
                foreground="white",
                fieldbackground="#333333",
            )
            if hasattr(self, "tab_hist") and hasattr(self.tab_hist, "tree_hist"):
                self.tab_hist.tree_hist.tag_configure(
                    "folder", background="#444444", foreground="#FFD700"
                )
        else:
            s.configure(
                "Treeview",
                background="white",
                foreground="black",
                fieldbackground="white",
            )
            if hasattr(self, "tab_hist") and hasattr(self.tab_hist, "tree_hist"):
                self.tab_hist.tree_hist.tag_configure(
                    "folder", background="#E3F2FD", foreground="black"
                )

        s.configure(
            "Primary.TButton",
            background=santander_red,
            foreground="white",
            borderwidth=0,
            font=("Segoe UI", 10, "bold"),
        )
        s.map("Primary.TButton", background=[("active", santander_dark_red)])
        s.configure(
            "Secondary.TButton",
            background=colors["border"],
            foreground=colors["fg"],
            borderwidth=0,
        )
        s.configure(
            "Danger.TButton",
            background=colors["danger"],
            foreground="white",
            borderwidth=0,
        )
        s.configure("TCheckbutton", background=colors["bg"], foreground=colors["fg"])
        s.map("TCheckbutton", background=[("active", colors["bg"])])
        s.configure(
            "UpdateExist.TButton",
            background="#0078D4",
            foreground="white",
            borderwidth=0,
        )

        try:
            self.header_bg.config(bg=santander_red)
            self.lbl_title.config(bg=santander_red, fg="white")
            self.chk_theme.config(
                bg=santander_red,
                fg="white",
                selectcolor=santander_red,
                activebackground=santander_red,
                activeforeground="white",
            )
            if hasattr(self, "fr_botoes_bg"):
                self.fr_botoes_bg.config(bg=colors["panel"])
            if hasattr(self, "tab_exec") and hasattr(self.tab_exec, "term"):
                self.tab_exec.term.config(
                    bg=colors["input"],
                    fg=colors["fg_dim"],
                    insertbackground=colors["fg"],
                )
        except:
            pass

    def _build_layout(self):
        self.header_bg = tk.Frame(self, height=70)
        self.header_bg.pack(fill="x", side="top")
        self.header_bg.pack_propagate(False)
        self.lbl_title = tk.Label(
            self.header_bg, text=" AUDIT ROBOT | Asset Management", font=self.font_h1
        )
        self.lbl_title.pack(side="left", padx=20, pady=10)
        self.chk_theme = tk.Checkbutton(
            self.header_bg,
            text="Dark Mode",
            variable=self.dark_mode,
            command=self.aplicar_tema,
            font=("Segoe UI", 10, "bold"),
        )
        self.chk_theme.pack(side="right", padx=20)

        main = ttk.Frame(self)
        main.pack(fill="both", expand=True, padx=20, pady=20)
        self.nb = ttk.Notebook(main)
        self.nb.pack(fill="both", expand=True)

        f_exec = ttk.Frame(self.nb)
        self.nb.add(f_exec, text=" Execução ")
        f_tool = ttk.Frame(self.nb)
        self.nb.add(f_tool, text=" Ferramentas ")
        f_hist = ttk.Frame(self.nb)
        self.nb.add(f_hist, text=" Base de Dados ")
        f_conf = ttk.Frame(self.nb)
        self.nb.add(f_conf, text=" Ajustes ")

        self.tab_exec = TabExecucao(f_exec, self)
        self.tab_exec.montar()

        self.tab_tools = TabFerramentas(f_tool, self)
        self.tab_tools.montar()

        self.tab_hist = TabHistorico(f_hist, self)
        self.tab_hist.montar()

        self.tab_conf = TabConfiguracao(f_conf, self)
        self.tab_conf.montar()

    def log(self, msg):
        if hasattr(self, "tab_exec"):
            self.tab_exec.log(msg)
        else:
            print(f"[LOG] {msg}")

    def salvar_cfg_interno(self):
        if hasattr(self, "tab_conf"):
            self.tab_conf.salvar_cfg_interno()

    def prevenir_suspensao(self, ativar=True):
        try:
            flag = 0x80000001 if ativar else 0x80000000
            ctypes.windll.kernel32.SetThreadExecutionState(flag)
        except Exception as e:
            print(f"Aviso: Falha de Suspensão: {e}")

    def sel_pasta(self):
        from tkinter import filedialog

        d = filedialog.askdirectory(title="Selecione a Pasta de Rede")
        if d:
            self.path_rede.set(d)
            self.salvar_cfg_interno()


# ==========================================
# ROTINAS DE INICIALIZAÇÃO (Vindas do main.py)
# ==========================================
def configurar_alta_resolucao():
    try:
        ctypes.windll.shcore.SetProcessDpiAwareness(1)
    except:
        try:
            ctypes.windll.user32.SetProcessDPIAware()
        except:
            pass


def hook_erro_global(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    erro_msg = "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
    print(erro_msg, file=sys.stderr)
    root = tk.Tk()
    root.withdraw()
    messagebox.showerror(
        "Erro Crítico Não Tratado",
        f"Ocorreu um erro inesperado:\n\n{exc_value}\n\n{erro_msg}",
    )
    root.destroy()
    sys.exit(1)


def verificar_dependencias():
    erros = []
    try:
        import selenium
    except:
        erros.append("selenium")
    if erros:
        root = tk.Tk()
        root.withdraw()
        messagebox.showerror(
            "Bibliotecas Faltando",
            f"Faltam bibliotecas: {', '.join(erros)}\nExecute: pip install {' '.join(erros)}",
        )
        sys.exit(1)


if __name__ == "__main__":
    sys.excepthook = hook_erro_global
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    configurar_alta_resolucao()
    verificar_dependencias()

    app = AuditApp()
    app.mainloop()
