import tkinter as tk
from tkinter import ttk, filedialog, messagebox, simpledialog
import threading
import os
from datetime import datetime
import csv

from tab_base import BaseTab


class TabHistorico(BaseTab):
    def __init__(self, parent, app):
        super().__init__(parent, app)

        # --- Variáveis Locais da Aba ---
        self.hist_filtro_dias = tk.StringVar(value="30 dias")
        self.hist_busca = tk.StringVar()
        self.db_historico_cache = []
        self.txt_hist_path_view = tk.StringVar()

        # Path local da aba — inicializa com o valor da aba Execução se disponível
        self.path_local = tk.StringVar(
            value=getattr(app, "path_rede", tk.StringVar()).get()
        )

        # --- Variáveis de Métricas ---
        self.lbl_metric_files = tk.StringVar(value="0")
        self.lbl_metric_lines = tk.StringVar(value="0")

        # --- Menu de Contexto ---
        self.menu_contexto = tk.Menu(self.parent, tearoff=0)
        self.menu_contexto.add_command(
            label="📄 Abrir Ficheiro Direto", command=self.abrir_arquivo_direto
        )
        self.menu_contexto.add_command(
            label="📂 Abrir Local na Rede", command=self.abrir_local_arquivo
        )

    # ==========================
    # HELPERS INTERNOS
    # ==========================
    def _cor(self, key, fallback="#333333"):
        """Acessa cores do tema de forma segura."""
        return (
            self.app.colors.get(key, fallback)
            if hasattr(self.app, "colors")
            else fallback
        )

    def _sync_path_from_exec(self):
        """Copia o path da aba Execução para o campo local."""
        val = self.app.path_rede.get() if hasattr(self.app, "path_rede") else ""
        if val:
            self.path_local.set(val)
            self.app.log("Path sincronizado da aba Execução.")
        else:
            messagebox.showinfo(
                "Sem path",
                "A aba Execução não tem nenhuma pasta configurada ainda.\n"
                "Digite o caminho manualmente ou use o botão 📁.",
            )

    def _browse_path(self):
        d = filedialog.askdirectory(title="Selecionar Pasta de Rede / Base de Dados")
        if d:
            self.path_local.set(d)
            # Também sincroniza de volta para o app
            if hasattr(self.app, "path_rede"):
                self.app.path_rede.set(d)

    def _get_path(self):
        """Retorna o path ativo: local > app.path_rede."""
        local = self.path_local.get().strip()
        if local:
            return local
        return (
            self.app.path_rede.get().strip() if hasattr(self.app, "path_rede") else ""
        )

    # ==========================
    # 1. CONSTRUÇÃO DA INTERFACE
    # ==========================
    def montar(self):
        # Frame raiz com padding
        root_frame = ttk.Frame(self.parent)
        root_frame.pack(fill="both", expand=True)

        # ── CABEÇALHO ───────────────────────────────────────────────────────────
        self._build_header(root_frame)

        # ── CORPO PRINCIPAL ──────────────────────────────────────────────────────
        body = ttk.Frame(root_frame)
        body.pack(fill="both", expand=True, padx=20, pady=(0, 10))

        # ── PAINEL DE CONTROLES (esquerda + direita) ─────────────────────────────
        ctrl_frame = ttk.LabelFrame(
            body,
            text="  Controles & Fonte de Dados  ",
            style="Card.TLabelframe",
            padding=(15, 10),
        )
        ctrl_frame.pack(fill="x", pady=(0, 12))

        self._build_path_row(ctrl_frame)
        self._build_filter_row(ctrl_frame)
        self._build_action_row(ctrl_frame)
        self._build_metrics_row(ctrl_frame)

        # Progressbar oculta (mostrada dinamicamente)
        self.prog_hist = ttk.Progressbar(ctrl_frame, mode="indeterminate")

        # ── TREEVIEW ─────────────────────────────────────────────────────────────
        self._build_tree(body)

        # ── RODAPÉ ───────────────────────────────────────────────────────────────
        self._build_footer(body)

    # ── SEÇÕES DE BUILD ──────────────────────────────────────────────────────────

    def _build_header(self, parent):
        """Barra de título com destaque visual."""
        hdr = tk.Frame(parent, height=52)
        hdr.pack(fill="x", side="top")
        hdr.pack_propagate(False)

        # Fundo com a cor accent do tema
        accent = self._cor("accent", "#EC0000")
        hdr.configure(bg=accent)

        tk.Label(
            hdr,
            text="  📂  Base de Dados & Volumetria",
            font=("Segoe UI", 13, "bold"),
            bg=accent,
            fg="white",
            anchor="w",
        ).pack(side="left", padx=18, fill="y")

        # Tag de versão / contexto
        tk.Label(
            hdr,
            text="CMDB · Audit Robot V11",
            font=("Segoe UI", 9),
            bg=accent,
            fg="#FFDDDD",
        ).pack(side="right", padx=18, fill="y")

    def _build_path_row(self, parent):
        """Linha dedicada ao input do caminho de rede."""
        row = ttk.Frame(parent, style="Card.TFrame")
        row.pack(fill="x", pady=(2, 8))

        ttk.Label(
            row,
            text="📡  Pasta de Rede:",
            style="Card.TLabel",
            font=("Segoe UI", 9, "bold"),
        ).pack(side="left", padx=(0, 8))

        ttk.Entry(
            row,
            textvariable=self.path_local,
            font=("Consolas", 9),
            width=55,
        ).pack(side="left", fill="x", expand=True, padx=(0, 6))

        ttk.Button(
            row,
            text="📁",
            width=3,
            command=self._browse_path,
        ).pack(side="left", padx=(0, 4))

        ttk.Button(
            row,
            text="↩ Usar path da Execução",
            style="Secondary.TButton",
            command=self._sync_path_from_exec,
        ).pack(side="left")

    def _build_filter_row(self, parent):
        """Linha com período, busca e botão principal de carga."""
        row = ttk.Frame(parent, style="Card.TFrame")
        row.pack(fill="x", pady=(0, 6))

        # Período
        ttk.Label(row, text="Período:", style="Card.TLabel").pack(side="left")
        self.cb_hist_filtro = ttk.Combobox(
            row,
            textvariable=self.hist_filtro_dias,
            values=["30 dias", "60 dias", "90 dias", "Todo o Período"],
            state="readonly",
            width=14,
        )
        self.cb_hist_filtro.pack(side="left", padx=(6, 16))

        # Botão principal
        ttk.Button(
            row,
            text="🔄  Carregar / Atualizar",
            style="Primary.TButton",
            command=self.refresh_history_db,
        ).pack(side="left", padx=(0, 20))

        # Separador visual
        ttk.Separator(row, orient="vertical").pack(
            side="left", fill="y", padx=6, pady=2
        )

        # Busca
        ttk.Label(row, text="🔍  Pesquisar:", style="Card.TLabel").pack(
            side="left", padx=(6, 4)
        )
        self.ent_hist_search = ttk.Entry(
            row, textvariable=self.hist_busca, font=("Segoe UI", 9), width=28
        )
        self.ent_hist_search.pack(side="left", fill="x", expand=True)
        self.ent_hist_search.bind("<KeyRelease>", lambda e: self.filtrar_history_view())

    def _build_action_row(self, parent):
        """Linha de ações secundárias (pendências, exportar, abrir)."""
        row = ttk.Frame(parent, style="Card.TFrame")
        row.pack(fill="x", pady=(0, 8))

        ttk.Button(
            row,
            text="📋  Analisar Pendências",
            style="Primary.TButton",
            command=self.view_missing_files,
        ).pack(side="left", padx=(0, 6))

        ttk.Button(
            row,
            text="📥  Exportar CSV",
            style="Secondary.TButton",
            command=self.exportar_historico_csv,
        ).pack(side="left", padx=(0, 6))

        ttk.Separator(row, orient="vertical").pack(
            side="left", fill="y", padx=8, pady=2
        )

        ttk.Button(
            row,
            text="📂  Abrir Local",
            style="Secondary.TButton",
            command=self.abrir_local_arquivo,
        ).pack(side="left", padx=(0, 6))

        ttk.Button(
            row,
            text="📄  Abrir Arquivo",
            style="Secondary.TButton",
            command=self.abrir_arquivo_direto,
        ).pack(side="left")

    def _build_metrics_row(self, parent):
        """Mini-cards de métricas inline."""
        row = tk.Frame(parent, bg=self._cor("bg", "#F4F4F4"))
        row.pack(fill="x", pady=(4, 2))

        self._metric_card(row, "📁 Arquivos Mapeados", self.lbl_metric_files, "#1565C0")
        self._metric_card(
            row, "📊 Volume de Registros", self.lbl_metric_lines, "#2E7D32"
        )

    def _metric_card(self, parent, label_text, var, accent_color):
        """Cria um mini-card de métrica."""
        panel_bg = self._cor("panel", "#FFFFFF")
        border = self._cor("border", "#DDDDDD")

        card = tk.Frame(parent, bg=panel_bg, relief="flat", bd=0)
        card.pack(side="left", padx=(0, 12), ipadx=14, ipady=6)

        # Borda esquerda colorida
        tk.Frame(card, bg=accent_color, width=4).pack(side="left", fill="y")

        inner = tk.Frame(card, bg=panel_bg)
        inner.pack(side="left", padx=(8, 4))

        tk.Label(
            inner,
            text=label_text,
            font=("Segoe UI", 8),
            bg=panel_bg,
            fg=self._cor("fg_dim", "#666666"),
        ).pack(anchor="w")

        tk.Label(
            inner,
            textvariable=var,
            font=("Segoe UI", 16, "bold"),
            bg=panel_bg,
            fg=accent_color,
        ).pack(anchor="w")

    def _build_tree(self, parent):
        """Treeview de arquivos com scrollbar e tags visuais."""
        # Frame com borda sutil
        wrapper = tk.Frame(
            parent,
            bg=self._cor("border", "#CCCCCC"),
            bd=1,
            relief="flat",
        )
        wrapper.pack(fill="both", expand=True, pady=(0, 6))

        tree_fr = ttk.Frame(wrapper)
        tree_fr.pack(fill="both", expand=True, padx=1, pady=1)

        cols = ("Linhas", "Data", "Status", "Path")
        self.tree_hist = ttk.Treeview(
            tree_fr,
            columns=cols,
            displaycolumns=("Linhas", "Data", "Status"),
            selectmode="browse",
        )

        self.tree_hist.heading("#0", text="  Mês / Arquivo", anchor="w")
        self.tree_hist.column("#0", width=440, minwidth=200)
        self.tree_hist.heading("Linhas", text="Registros")
        self.tree_hist.column("Linhas", width=110, anchor="center", stretch=False)
        self.tree_hist.heading("Data", text="Modificação")
        self.tree_hist.column("Data", width=155, anchor="center", stretch=False)
        self.tree_hist.heading("Status", text="Tag")
        self.tree_hist.column("Status", width=80, anchor="center", stretch=False)

        # Tags visuais
        self.tree_hist.tag_configure(
            "folder",
            font=("Segoe UI", 10, "bold"),
            background="#EFF6FF",
            foreground="#1E40AF",
        )
        self.tree_hist.tag_configure("new", foreground="#166534", background="#F0FDF4")
        self.tree_hist.tag_configure("old", foreground="#991B1B", background="#FFF5F5")

        self.tree_hist.bind("<Double-1>", self.abrir_arquivo_direto)
        self.tree_hist.bind("<Button-3>", self.abrir_menu_contexto)

        sb = ttk.Scrollbar(tree_fr, orient="vertical", command=self.tree_hist.yview)
        self.tree_hist.configure(yscrollcommand=sb.set)
        self.tree_hist.pack(side="left", fill="both", expand=True)
        sb.pack(side="right", fill="y")

    def _build_footer(self, parent):
        """Rodapé com ação legada."""
        bot = ttk.Frame(parent)
        bot.pack(fill="x", pady=(4, 0))

        ttk.Button(
            bot,
            text="Importar Log Externo (Legado)",
            command=self.carregar_historico_view_csv,
            style="Secondary.TButton",
        ).pack(side="right")

    # ==========================
    # 2. LÓGICA DO HISTÓRICO
    # ==========================
    def carregar_historico_view_csv(self):
        f = filedialog.askopenfilename(filetypes=[("CSV", "*.csv")])
        if f:
            self.tree_hist.delete(*self.tree_hist.get_children())
            self.txt_hist_path_view.set(os.path.basename(f))
            try:
                with open(f, "r", encoding="utf-8", errors="ignore") as fi:
                    reader = csv.reader(fi)
                    next(reader, None)
                    for row in reader:
                        if row:
                            self.tree_hist.insert("", "end", values=row)
            except Exception:
                pass

    def refresh_history_db(self):
        p = self._get_path()
        if not p:
            return messagebox.showwarning(
                "Pasta não configurada",
                "Informe o caminho da pasta de rede no campo acima\n"
                "ou use '↩ Usar path da Execução' para importar da aba Execução.",
            )
        if not os.path.exists(p):
            return messagebox.showerror(
                "Pasta inacessível",
                f"O caminho abaixo não foi encontrado ou está offline:\n\n{p}",
            )

        # Sincroniza de volta ao app para consistência
        if hasattr(self.app, "path_rede"):
            self.app.path_rede.set(p)

        self.prog_hist.pack(fill="x", pady=(6, 2))
        self.prog_hist.start(10)

        def worker():
            try:
                filtro_str = self.hist_filtro_dias.get()
                dias = 30
                if "60" in filtro_str:
                    dias = 60
                elif "90" in filtro_str:
                    dias = 90
                elif "Todo" in filtro_str:
                    dias = None

                dados, total = self.app.data_processor.listar_historico(p, dias)
                self.app.after(0, lambda: self._update_hist_cache(dados, total))
            except Exception as e:
                import traceback

                traceback.print_exc()
                self.app.after(0, lambda err=e: messagebox.showerror("Erro", str(err)))
            finally:
                self.app.after(
                    0, lambda: [self.prog_hist.stop(), self.prog_hist.pack_forget()]
                )

        threading.Thread(target=worker, daemon=True).start()

    def _update_hist_cache(self, dados, total):
        self.db_historico_cache = dados
        self.app.after(0, self.filtrar_history_view)

    def filtrar_history_view(self):
        self.tree_hist.delete(*self.tree_hist.get_children())
        termo = self.hist_busca.get().lower()
        grupos = {}

        for item in self.db_historico_cache:
            if termo and termo not in item["nome"].lower():
                continue
            grupos.setdefault(item["pasta_pai"], []).append(item)

        total_files = 0
        total_all_lines = 0

        for pasta in sorted(grupos.keys(), reverse=True):
            items = grupos[pasta]
            total_linhas = sum(
                int(i["linhas"]) for i in items if str(i["linhas"]).isdigit()
            )

            total_files += len(items)
            total_all_lines += total_linhas

            texto_pasta = f"  {pasta}   ({total_linhas:,} registros)".replace(",", ".")
            folder_id = self.tree_hist.insert(
                "", "end", text=texto_pasta, open=True, tags=("folder",)
            )
            for i in items:
                self.tree_hist.insert(
                    folder_id,
                    "end",
                    text=f"  {i['nome']}",
                    values=(i["linhas"], i["data"], i["tag"], i["caminho"]),
                    tags=(i["tag"],),
                )

        self.lbl_metric_files.set(f"{total_files:,}".replace(",", "."))
        self.lbl_metric_lines.set(f"{total_all_lines:,}".replace(",", "."))

    # --- Ações de Arquivo ---
    def abrir_menu_contexto(self, e):
        try:
            self.menu_contexto.tk_popup(e.x_root, e.y_root)
        finally:
            self.menu_contexto.grab_release()

    def abrir_arquivo_direto(self, event=None):
        sel = self.tree_hist.selection()
        if sel:
            try:
                item = self.tree_hist.item(sel[0])
                valores = item["values"]
                if valores and len(valores) >= 4:
                    caminho_arquivo = valores[3]
                    if os.path.exists(caminho_arquivo):
                        os.startfile(caminho_arquivo)
                    else:
                        messagebox.showwarning(
                            "Aviso",
                            "O ficheiro pode ter sido movido ou excluído da rede.",
                        )
            except Exception as e:
                print(f"Erro ao abrir arquivo: {e}")

    def abrir_local_arquivo(self):
        sel = self.tree_hist.selection()
        if sel:
            try:
                item = self.tree_hist.item(sel[0])
                valores = item["values"]
                if valores and len(valores) >= 4:
                    caminho_arquivo = valores[3]
                    if os.path.exists(caminho_arquivo):
                        pasta = os.path.dirname(caminho_arquivo)
                        os.startfile(pasta)
                    else:
                        messagebox.showwarning(
                            "Aviso", "O ficheiro ou pasta não foi encontrado na rede."
                        )
            except Exception as e:
                print(f"Erro ao abrir pasta: {e}")

    # --- Exportação ---
    def exportar_historico_csv(self):
        if not self.db_historico_cache:
            return messagebox.showwarning(
                "Aviso",
                "A base está vazia. Clique em '🔄 Carregar / Atualizar' primeiro.",
            )

        f = filedialog.asksaveasfilename(
            title="Salvar Relatório de Volumetria",
            defaultextension=".csv",
            filetypes=[("CSV (Excel)", "*.csv")],
            initialfile=f"Relatorio_Volumetria_CMDB_{datetime.now().strftime('%d%m%Y')}.csv",
        )

        if f:
            try:
                with open(f, "w", encoding="utf-8-sig", newline="") as csvfile:
                    writer = csv.writer(csvfile, delimiter=";")
                    writer.writerow(
                        [
                            "Agrupamento (Mês)",
                            "Nome do Arquivo",
                            "Volume de Linhas",
                            "Data de Modificação",
                            "Status Idade",
                            "Caminho na Rede",
                        ]
                    )
                    for item in self.db_historico_cache:
                        writer.writerow(
                            [
                                item.get("pasta_pai", ""),
                                item.get("nome", ""),
                                item.get("linhas", 0),
                                item.get("data", ""),
                                item.get("tag", ""),
                                item.get("caminho", ""),
                            ]
                        )
                messagebox.showinfo(
                    "Sucesso",
                    "Relatório exportado com sucesso!\nJá pode utilizá-lo para dashboards ou auditorias.",
                )
                os.startfile(f)
            except Exception as e:
                messagebox.showerror("Erro de I/O", f"Falha ao exportar:\n{e}")

    # --- Pendências ---
    def view_missing_files(self):
        p = self._get_path()
        if not p or not os.path.exists(p):
            return messagebox.showwarning(
                "Aviso",
                "Configure uma pasta de rede válida no campo acima antes de verificar pendências.",
            )

        lista_atual = (
            self.app.tab_exec.lista_exec if hasattr(self.app, "tab_exec") else []
        )

        if not lista_atual:
            return messagebox.showwarning(
                "Aviso",
                "A lista de ficheiros modelo está vazia.\n\n"
                "Execute '🔍 Buscar Links' na aba Execução primeiro.",
            )

        mes_atual = datetime.now().strftime("%m.%Y")
        m = simpledialog.askstring(
            "Verificar Pendências",
            "Qual Mês/Ano quer validar? (ex: 02.2026)\n"
            "(O sistema cruzará o que deveria existir vs o que realmente existe)",
            parent=self.app,
            initialvalue=mes_atual,
        )

        if m:
            nomes_esperados = [item["name"] for item in lista_atual]
            if hasattr(self.app, "tab_exec"):
                self.app.tab_exec.txt_status_busca.set("Verificando pendências...")

            threading.Thread(
                target=lambda: self._thread_missing(p, m, nomes_esperados), daemon=True
            ).start()

    def _thread_missing(self, root_path, mes_alvo, lista_nomes):
        try:
            lista_faltantes, caminho_final = (
                self.app.data_processor.verificar_pendencias(
                    root_path, mes_alvo, lista_nomes
                )
            )

            if hasattr(self.app, "tab_exec"):
                self.app.after(
                    0, lambda: self.app.tab_exec.txt_status_busca.set("Pronto")
                )

            caminho_msg = (
                caminho_final
                if caminho_final
                else f"Pasta '{mes_alvo}' não encontrada na raiz."
            )

            if lista_faltantes:
                self.app.after(
                    0,
                    lambda: self._show_missing_popup(
                        mes_alvo, lista_faltantes, caminho_msg
                    ),
                )
            else:
                msg = (
                    f"✅ Tudo certo!\n\n"
                    f"Todos os {len(lista_nomes)} ficheiros da lista estão presentes no mês {mes_alvo}:\n\n"
                    f"{caminho_msg}"
                )
                self.app.after(
                    0, lambda: messagebox.showinfo("Excelência de Dados ✅", msg)
                )
        except Exception as e:
            self.app.after(
                0,
                lambda err=e: messagebox.showerror(
                    "Erro Técnico", f"Erro ao verificar pendências: {err}"
                ),
            )

    def _show_missing_popup(self, mes, lista, caminho):
        top = tk.Toplevel(self.app)
        top.title(f"Relatório de Pendências — {mes}")
        top.geometry("720x620")
        top.configure(bg=self.app.colors["panel"])

        # Cabeçalho do popup
        hdr = tk.Frame(top, bg=self._cor("danger", "#D32F2F"), height=50)
        hdr.pack(fill="x")
        hdr.pack_propagate(False)
        tk.Label(
            hdr,
            text=f"  ❌  {len(lista)} ficheiro(s) pendente(s) para o mês {mes}",
            font=("Segoe UI", 11, "bold"),
            bg=self._cor("danger", "#D32F2F"),
            fg="white",
            anchor="w",
        ).pack(fill="both", padx=10)

        # Caminho pesquisado
        f_info = ttk.Frame(top, padding=(12, 6))
        f_info.pack(fill="x")
        ttk.Label(
            f_info,
            text=f"Base de busca:  {caminho}",
            font=("Segoe UI", 8, "italic"),
            foreground=self._cor("fg_dim", "#666"),
            wraplength=680,
        ).pack(anchor="w")

        # Lista
        f_list = tk.Frame(top, bg=self._cor("input", "#FAFAFA"), padx=12, pady=8)
        f_list.pack(fill="both", expand=True, padx=12, pady=(4, 8))

        t = tk.Text(
            f_list,
            bg=self._cor("input", "#FAFAFA"),
            fg=self._cor("fg", "#333"),
            font=("Consolas", 10),
            relief="flat",
            padx=6,
            pady=6,
        )
        sb = ttk.Scrollbar(f_list, orient="vertical", command=t.yview)
        t.configure(yscrollcommand=sb.set)
        t.pack(side="left", fill="both", expand=True)
        sb.pack(side="right", fill="y")

        for i in sorted(lista):
            t.insert("end", f"• {i}\n")
        t.config(state="disabled")

        # Botões
        f_btn = ttk.Frame(top, padding=(12, 8))
        f_btn.pack(fill="x")

        def copiar_lista():
            top.clipboard_clear()
            top.clipboard_append("\n".join(sorted(lista)))
            messagebox.showinfo(
                "Copiado ✅",
                "Lista de ficheiros pendentes copiada para a área de transferência.",
                parent=top,
            )

        ttk.Button(
            f_btn,
            text="📋  Copiar Lista para E-mail",
            style="Primary.TButton",
            command=copiar_lista,
        ).pack(side="left", padx=(0, 8))

        ttk.Button(
            f_btn,
            text="Fechar",
            style="Secondary.TButton",
            command=top.destroy,
        ).pack(side="left")
