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

        # --- Variáveis de Métricas (Business Value) ---
        self.lbl_metric_files = tk.StringVar(value="Arquivos Mapeados: 0")
        self.lbl_metric_lines = tk.StringVar(value="Linhas Totais Processadas: 0")

        # --- Menu de Contexto ---
        self.menu_contexto = tk.Menu(self.parent, tearoff=0)
        self.menu_contexto.add_command(
            label="📄 Abrir Ficheiro Direto", command=self.abrir_arquivo_direto
        )
        self.menu_contexto.add_command(
            label="📂 Abrir Local na Rede", command=self.abrir_local_arquivo
        )

    # ==========================
    # 1. CONSTRUÇÃO DA INTERFACE
    # ==========================
    def montar(self):
        container = ttk.Frame(self.parent)
        container.pack(fill="both", expand=True, padx=20, pady=20)

        fr_top = ttk.LabelFrame(
            container,
            text=" Gestão da Base de Dados & Métricas ",
            style="Card.TLabelframe",
            padding=15,
        )
        fr_top.pack(fill="x", pady=(0, 10))

        # --- Linha 1: Buscas e Filtros ---
        f_in = ttk.Frame(fr_top, style="Card.TFrame")
        f_in.pack(fill="x", pady=(0, 5))

        ttk.Label(f_in, text="Período:", style="Card.TLabel").pack(side="left")
        self.cb_hist_filtro = ttk.Combobox(
            f_in,
            textvariable=self.hist_filtro_dias,
            values=["30 dias", "60 dias", "90 dias", "Todo o Período"],
            state="readonly",
            width=15,
        )
        self.cb_hist_filtro.pack(side="left", padx=10)

        ttk.Button(
            f_in,
            text="🔄 Carregar / Atualizar Base",
            command=self.refresh_history_db,
            style="Primary.TButton",
        ).pack(side="left", padx=10)

        ttk.Label(f_in, text="🔍 Pesquisar (Nome):", style="Card.TLabel").pack(
            side="left", padx=(20, 5)
        )
        self.ent_hist_search = ttk.Entry(
            f_in, textvariable=self.hist_busca, font=self.app.font_body
        )
        self.ent_hist_search.pack(side="left", fill="x", expand=True)
        self.ent_hist_search.bind("<KeyRelease>", lambda e: self.filtrar_history_view())

        # --- Linha 2: Ações de Valor e Exportação ---
        f_actions = ttk.Frame(fr_top, style="Card.TFrame")
        f_actions.pack(fill="x", pady=10)

        ttk.Button(
            f_actions,
            text="📋 Analisar Pendências (Mês)",
            command=self.view_missing_files,
            style="Primary.TButton",
        ).pack(side="left", padx=(0, 10))

        ttk.Button(
            f_actions,
            text="📥 Exportar Relatório (CSV)",
            command=self.exportar_historico_csv,
            style="Secondary.TButton",
        ).pack(side="left", padx=10)

        # Resgate das Cores de Forma Segura usando .get()
        cor_borda = (
            self.app.colors.get("border", "#CCC")
            if hasattr(self.app, "colors")
            else "#CCC"
        )
        ttk.Label(f_actions, text="|", foreground=cor_borda).pack(side="left", padx=10)

        ttk.Button(
            f_actions,
            text="📂 Abrir Local",
            command=self.abrir_local_arquivo,
            style="Secondary.TButton",
        ).pack(side="left", padx=10)

        ttk.Button(
            f_actions,
            text="📄 Abrir Arquivo",
            command=self.abrir_arquivo_direto,
            style="Secondary.TButton",
        ).pack(side="left", padx=10)

        # --- Linha 3: Dashboard de Métricas ---
        f_dash = ttk.Frame(fr_top, style="Card.TFrame")
        f_dash.pack(fill="x", pady=(5, 0))

        cor_accent = (
            self.app.colors.get("accent", "#EC0000")
            if hasattr(self.app, "colors")
            else "#EC0000"
        )
        cor_success = (
            self.app.colors.get("success", "#2E7D32")
            if hasattr(self.app, "colors")
            else "#2E7D32"
        )

        ttk.Label(
            f_dash,
            textvariable=self.lbl_metric_files,
            font=self.app.font_h2,
            foreground=cor_accent,
        ).pack(side="left", padx=(0, 20))

        ttk.Label(
            f_dash,
            textvariable=self.lbl_metric_lines,
            font=self.app.font_h2,
            foreground=cor_success,
        ).pack(side="left")

        # Barra de Progresso Oculta por padrão (Será mostrada via .pack() dinâmico)
        self.prog_hist = ttk.Progressbar(fr_top, mode="indeterminate")

        # --- Tabela Visual ---
        tree_fr = ttk.Frame(container)
        tree_fr.pack(fill="both", expand=True)

        cols = ("Linhas", "Data", "Status", "Path")
        self.tree_hist = ttk.Treeview(
            tree_fr, columns=cols, displaycolumns=("Linhas", "Data", "Status")
        )

        self.tree_hist.heading("#0", text="Mês / Arquivo", anchor="w")
        self.tree_hist.column("#0", width=450)
        self.tree_hist.heading("Linhas", text="Registros")
        self.tree_hist.column("Linhas", width=100, anchor="center")
        self.tree_hist.heading("Data", text="Modificação")
        self.tree_hist.column("Data", width=150, anchor="center")
        self.tree_hist.heading("Status", text="Tag")
        self.tree_hist.column("Status", width=100, anchor="center")

        self.tree_hist.tag_configure(
            "folder", font=("Segoe UI", 10, "bold"), background="#E3F2FD"
        )
        self.tree_hist.tag_configure("new", foreground="#2E7D32")
        self.tree_hist.tag_configure("old", foreground="#C62828")

        # Usabilidade: Binding de duplo clique e botão direito
        self.tree_hist.bind("<Double-1>", self.abrir_arquivo_direto)
        self.tree_hist.bind("<Button-3>", self.abrir_menu_contexto)

        sb = ttk.Scrollbar(tree_fr, orient="vertical", command=self.tree_hist.yview)
        self.tree_hist.configure(yscrollcommand=sb.set)
        self.tree_hist.pack(side="left", fill="both", expand=True)
        sb.pack(side="right", fill="y")

        # Rodapé Legado
        bot = ttk.Frame(container)
        bot.pack(fill="x", pady=10)
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
            except:
                pass

    def refresh_history_db(self):
        p = self.app.path_rede.get()
        if not p:
            return messagebox.showwarning(
                "Aviso", "Selecione uma pasta de rede na aba Execução."
            )

        # Exibe a barra dinamicamente
        self.prog_hist.pack(fill="x", pady=(10, 0))
        self.prog_hist.start(10)

        def worker():
            try:
                # Extrai o número de dias do Combobox
                filtro_str = self.hist_filtro_dias.get()
                dias = 30
                if "60" in filtro_str:
                    dias = 60
                elif "90" in filtro_str:
                    dias = 90
                elif "Todo" in filtro_str:
                    dias = None

                # Integração com DataProcessor
                dados, total = self.app.data_processor.listar_historico(p, dias)
                self.app.after(0, lambda: self._update_hist_cache(dados, total))
            except Exception as e:
                import traceback

                traceback.print_exc()
                self.app.after(0, lambda: messagebox.showerror("Erro", str(e)))
            finally:
                # Garante que a barra some mesmo se der erro
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

        # Filtra os dados
        for item in self.db_historico_cache:
            if termo and termo not in item["nome"].lower():
                continue
            grupos.setdefault(item["pasta_pai"], []).append(item)

        total_files = 0
        total_all_lines = 0

        # Constrói a árvore e calcula as métricas visuais
        for pasta in sorted(grupos.keys(), reverse=True):
            items = grupos[pasta]
            total_linhas = sum(
                int(i["linhas"]) for i in items if str(i["linhas"]).isdigit()
            )

            total_files += len(items)
            total_all_lines += total_linhas

            texto_pasta = f"{pasta} (Total: {total_linhas:,} linhas)".replace(",", ".")
            folder_id = self.tree_hist.insert(
                "", "end", text=texto_pasta, open=True, tags=("folder",)
            )
            for i in items:
                self.tree_hist.insert(
                    folder_id,
                    "end",
                    text=i["nome"],
                    values=(i["linhas"], i["data"], i["tag"], i["caminho"]),
                    tags=(i["tag"],),
                )

        # Atualiza o Dashboard de Negócio
        self.lbl_metric_files.set(f"Arquivos Mapeados: {total_files}")
        self.lbl_metric_lines.set(
            f"Volume Total de Registros: {total_all_lines:,}".replace(",", ".")
        )

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

    # --- Exportação de Negócio ---
    def exportar_historico_csv(self):
        if not self.db_historico_cache:
            return messagebox.showwarning(
                "Aviso", "A base está vazia. Clique em 'Carregar Base' primeiro."
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

    # --- LÓGICA DE PENDÊNCIAS ---
    def view_missing_files(self):
        p = self.app.path_rede.get()
        if not p or not os.path.exists(p):
            return messagebox.showwarning(
                "Aviso", "Selecione uma pasta de rede válida na aba 'Execução'."
            )

        # PONTE: Acessa a lista_exec da aba de Execução de forma segura
        lista_atual = (
            self.app.tab_exec.lista_exec if hasattr(self.app, "tab_exec") else []
        )

        if not lista_atual:
            return messagebox.showwarning(
                "Aviso",
                "A lista de ficheiros modelo está vazia.\n\nExecute '🔍 Buscar Links' primeiro na aba Execução para sabermos o que comparar.",
            )

        mes_atual = datetime.now().strftime("%m.%Y")
        m = simpledialog.askstring(
            "Verificar Pendências",
            "Qual Mês/Ano quer validar? (ex: 02.2026)\n(O sistema cruzará o que deveria existir vs o que realmente existe)",
            parent=self.app,
            initialvalue=mes_atual,
        )

        if m:
            nomes_esperados = [item["name"] for item in lista_atual]
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

            self.app.after(0, lambda: self.app.tab_exec.txt_status_busca.set("Pronto"))
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
                msg = f"Tudo certo!\nTodos os {len(lista_nomes)} ficheiros da lista estão presentes no mês {mes_alvo}:\n\n{caminho_msg}"
                self.app.after(
                    0, lambda: messagebox.showinfo("Excelência de Dados", msg)
                )
        except Exception as e:
            self.app.after(
                0,
                lambda: messagebox.showerror(
                    "Erro Técnico", f"Erro ao verificar pendências: {e}"
                ),
            )

    def _show_missing_popup(self, mes, lista, caminho):
        top = tk.Toplevel(self.app)
        top.title(f"Relatório de Pendências: {mes}")
        top.geometry("700x600")
        top.configure(bg=self.app.colors["panel"])

        f_top = ttk.Frame(top, padding=10)
        f_top.pack(fill="x")
        ttk.Label(
            f_top,
            text=f"❌ Faltam baixar {len(lista)} ficheiros para fechar o mês",
            font=("Segoe UI", 14, "bold"),
            foreground=self.app.colors["danger"],
        ).pack(anchor="w")
        ttk.Label(
            f_top,
            text=f"Base de busca: {caminho}",
            font=("Segoe UI", 9),
            wraplength=680,
        ).pack(anchor="w", pady=(5, 0))

        f_list = ttk.Frame(top, padding=10)
        f_list.pack(fill="both", expand=True)
        t = tk.Text(
            f_list,
            bg=self.app.colors["input"],
            fg=self.app.colors["fg"],
            font=("Consolas", 10),
        )
        sb = ttk.Scrollbar(f_list, orient="vertical", command=t.yview)
        t.configure(yscrollcommand=sb.set)
        t.pack(side="left", fill="both", expand=True)
        sb.pack(side="right", fill="y")

        for i in sorted(lista):
            t.insert("end", f"{i}\n")

        def copiar_lista():
            top.clipboard_clear()
            top.clipboard_append("\n".join(lista))
            messagebox.showinfo(
                "Copiado",
                "Lista de ficheiros pendentes copiada para a área de transferência.",
                parent=top,
            )

        ttk.Button(
            top,
            text="Copiar Lista para E-mail",
            command=copiar_lista,
            style="Primary.TButton",
        ).pack(pady=10)
