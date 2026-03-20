import tkinter as tk
from tkinter import ttk, messagebox
import threading
import time
import os
import re
import webbrowser
from datetime import datetime, timedelta
import winsound

from tab_base import BaseTab
from audit_report import AuditReporter


class TabExecucao(BaseTab):
    def __init__(self, parent, app):
        super().__init__(parent, app)

        # --- Variáveis de Estado da Aba ---
        self.lista_exec = []
        self.decisao_usuario = tk.StringVar(value="")
        self.start_time_exec = 0

        # --- Variáveis do Tkinter (UI) ---
        self.pular_existentes = tk.BooleanVar(value=False)
        self.txt_eta = tk.StringVar(value="Calculando ETA...")
        self.txt_status_busca = tk.StringVar(value="Aguardando...")
        self.txt_total_encontrados = tk.StringVar(value="0 Links")

        # --- Menu de Contexto ---
        self.menu_contexto = tk.Menu(self.parent, tearoff=0)
        self.menu_contexto.add_command(
            label="☑ Marcar Selecionados",
            command=lambda: self.batch_select_all(True, only_highlighted=True),
        )
        self.menu_contexto.add_command(
            label="☐ Desmarcar Selecionados",
            command=lambda: self.batch_select_all(False, only_highlighted=True),
        )
        self.menu_contexto.add_separator()
        self.menu_contexto.add_command(
            label="🌍 Abrir Link", command=self.abrir_link_selecionado
        )

    # ==========================
    # 1. CONSTRUÇÃO DA INTERFACE
    # ==========================
    def montar(self):
        container = ttk.Frame(self.parent)
        container.pack(fill="both", expand=True)

        # Barra de Ação Inferior (Fixa)
        self.bot_frame = tk.Frame(container, height=90)
        self.bot_frame.pack(side="bottom", fill="x")
        self.bot_frame.pack_propagate(False)

        fr_b = tk.Frame(self.bot_frame, padx=20, pady=20)
        fr_b.pack(fill="both", expand=True)
        fr_b.columnconfigure((0, 1, 2), weight=1)

        self.btn_iniciar = ttk.Button(
            fr_b,
            text="▶ INICIAR AUDITORIA",
            style="Primary.TButton",
            command=self.iniciar,
        )
        self.btn_iniciar.grid(row=0, column=0, sticky="nsew", padx=10)

        self.btn_pausar = ttk.Button(
            fr_b,
            text="⏸ PAUSAR",
            style="Secondary.TButton",
            command=self.toggle_pause,
            state="disabled",
        )
        self.btn_pausar.grid(row=0, column=1, sticky="nsew", padx=10)

        self.btn_parar = ttk.Button(
            fr_b,
            text="⏹ PARAR TUDO",
            style="Danger.TButton",
            command=self._confirmar_parada,
            state="disabled",
        )
        self.btn_parar.grid(row=0, column=2, sticky="nsew", padx=10)

        self.app.fr_botoes_bg = fr_b  # Mantido para o dark mode do app principal

        # Conteúdo Superior
        content = ttk.Frame(container)
        content.pack(side="top", fill="both", expand=True, padx=10, pady=10)

        # Barra de Busca
        fr_search = ttk.LabelFrame(
            content, text=" Fonte de Dados ", style="Card.TLabelframe", padding=10
        )
        fr_search.pack(fill="x", pady=(0, 10))
        fr_search.columnconfigure(0, weight=1)

        f_s_inner = ttk.Frame(fr_search, style="Card.TFrame")
        f_s_inner.pack(fill="x")

        ttk.Entry(
            f_s_inner, textvariable=self.app.path_rede, font=self.app.font_body
        ).pack(side="left", fill="x", expand=True, padx=(0, 5))

        ttk.Button(
            f_s_inner, text="📂 Selecionar Pasta", command=self.app.sel_pasta
        ).pack(side="left", padx=5)

        ttk.Button(
            f_s_inner,
            text="🔍 Buscar Links (.txt)",
            style="Primary.TButton",
            command=self.scan_links,
        ).pack(side="left", padx=5)

        f_s_info = ttk.Frame(fr_search, style="Card.TFrame")
        f_s_info.pack(fill="x", pady=(5, 0))

        ttk.Label(
            f_s_info,
            textvariable=self.txt_total_encontrados,
            font=self.app.font_h2,
            style="Card.TLabel",
        ).pack(side="left")

        ttk.Checkbutton(
            f_s_info,
            text="Pular arquivos já baixados",
            variable=self.pular_existentes,
            style="Card.TLabel",
        ).pack(side="right")

        # Botões de Seleção e Ação
        f_sel = ttk.Frame(content)
        f_sel.pack(fill="x", pady=(0, 5))

        ttk.Button(
            f_sel,
            text="☑ Marca Todos",
            width=15,
            command=lambda: self.batch_select_all(True),
        ).pack(side="left", padx=(0, 5))
        ttk.Button(
            f_sel,
            text="☐ Desmarca Todos",
            width=15,
            command=lambda: self.batch_select_all(False),
        ).pack(side="left", padx=(0, 20))
        ttk.Button(
            f_sel,
            text="🔄 Atualizar Existência",
            style="UpdateExist.TButton",
            command=self.atualizar_existencia_em_lote,
        ).pack(side="left", padx=(0, 10))
        ttk.Button(
            f_sel, text="🌍 Abrir Link Selecionado", command=self.abrir_link_selecionado
        ).pack(side="left")

        # Tabela (Treeview)
        tree_frame = ttk.Frame(content)
        tree_frame.pack(fill="both", expand=True)

        cols = ("Sel", "Nome", "Status", "Existência", "Tempo_DL", "Linhas")
        self.tree = ttk.Treeview(
            tree_frame, columns=cols, show="headings", selectmode="extended"
        )
        sb = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=sb.set)

        self.tree.heading(
            "Sel", text="[x]", command=lambda: self.batch_select_all(True)
        )
        self.tree.column("Sel", width=40, anchor="center", stretch=False)
        self.tree.heading("Nome", text="Nome Arquivo")
        self.tree.column("Nome", width=250, minwidth=150)
        self.tree.heading("Status", text="Status")
        self.tree.column("Status", width=90, anchor="center")
        self.tree.heading("Existência", text="Existência")
        self.tree.column("Existência", width=120, anchor="center")
        self.tree.heading("Tempo_DL", text="Tempo Total")
        self.tree.column("Tempo_DL", width=90, anchor="center")
        self.tree.heading("Linhas", text="Linhas")
        self.tree.column("Linhas", width=70, anchor="center")

        self.tree.pack(side="left", fill="both", expand=True)
        sb.pack(side="right", fill="y")

        self.tree.tag_configure("ok", foreground="#388E3C")
        self.tree.tag_configure("err", foreground="#D32F2F")
        self.tree.tag_configure("run", foreground="#1976D2")
        self.tree.tag_configure("empty", foreground="#F57C00")
        self.tree.tag_configure("wait", foreground="#7B1FA2")

        self.tree.bind("<Button-1>", self.on_tree_click_single)
        self.tree.bind("<Button-3>", self.abrir_menu_contexto)
        self.tree.bind("<Double-1>", lambda e: self.abrir_link_selecionado())

        # =======================================================
        # Ocultamos as barras inicialmente para o visual Clean
        # =======================================================
        bot_info = ttk.Frame(content)
        bot_info.pack(fill="x", pady=10)

        self.f_prog = ttk.Frame(bot_info)
        ttk.Label(self.f_prog, text="Progresso Geral:", font=self.app.font_h2).pack(
            side="left"
        )
        ttk.Label(self.f_prog, textvariable=self.txt_eta, font=self.app.font_body).pack(
            side="right"
        )

        self.prog_exec = ttk.Progressbar(bot_info, mode="determinate", length=200)
        self.prog_scan = ttk.Progressbar(bot_info, mode="indeterminate")

        self.term = tk.Text(
            bot_info,
            height=6,
            font=self.app.font_mono,
            state="disabled",
            padx=5,
            pady=5,
        )
        self.term.pack(fill="both", expand=True)

    # ==========================
    # 2. LÓGICA DE EXECUÇÃO
    # ==========================
    def iniciar(self):
        itens = [i for i in self.lista_exec if i["sel"]]
        if not itens:
            return messagebox.showwarning("Atenção", "Selecione itens na tabela.")

        if not self.app.motor.conectar_driver():
            return messagebox.showerror(
                "Erro",
                "Chrome não conectado.\nVerifique se o navegador está aberto em modo debug (Porta 9222).",
            )

        self.app.salvar_cfg_interno()
        self.app.logger.sessao_atual = []

        # Lança a contagem regressiva profissional
        self._show_countdown_popup()

    def _show_countdown_popup(self):
        """Pop-up flutuante de 3 segundos antes do robô iniciar os cliques."""
        top = tk.Toplevel(self.app)
        top.title("Preparando...")
        top.geometry("350x180")
        top.configure(bg=self.app.colors["panel"])

        # Centraliza o pop-up
        top.update_idletasks()
        x = self.app.winfo_x() + (self.app.winfo_width() // 2) - (350 // 2)
        y = self.app.winfo_y() + (self.app.winfo_height() // 2) - (180 // 2)
        top.geometry(f"+{x}+{y}")

        top.attributes("-topmost", True)
        top.overrideredirect(True)  # Remove as bordas feias do windows

        # Borda de destaque e fundo
        f_border = tk.Frame(top, bg=self.app.colors["accent"], bd=2)
        f_border.pack(fill="both", expand=True)
        f_inner = tk.Frame(f_border, bg=self.app.colors["panel"])
        f_inner.pack(fill="both", expand=True, padx=2, pady=2)

        tk.Label(
            f_inner,
            text="Solte o mouse e o teclado!",
            font=("Segoe UI", 12, "bold"),
            bg=self.app.colors["panel"],
            fg=self.app.colors["danger"],
        ).pack(pady=(20, 5))

        tk.Label(
            f_inner,
            text="O robô iniciará em:",
            font=("Segoe UI", 10),
            bg=self.app.colors["panel"],
            fg=self.app.colors["fg"],
        ).pack()

        lbl_count = tk.Label(
            f_inner,
            text="3",
            font=("Segoe UI", 48, "bold"),
            bg=self.app.colors["panel"],
            fg=self.app.colors["accent"],
        )
        lbl_count.pack(pady=(0, 10))

        def _count(n):
            if n > 0:
                lbl_count.config(text=str(n))
                self.app.after(1000, lambda: _count(n - 1))
            else:
                top.destroy()
                self.disparar_thread_robo()

        _count(3)

    def disparar_thread_robo(self):
        if self.app.motor.rodando_event.is_set():
            self.log("Já existe uma execução em andamento.")
            return

        itens = [i for i in self.lista_exec if i["sel"]]

        self.app.motor.rodando_event.set()
        self.app.motor.pausado_event.clear()

        self._update_ui_state(running=True)

        # Exibe as barras de progresso do robô dinamicamente
        self.f_prog.pack(fill="x", before=self.term)
        self.prog_exec.pack(fill="x", pady=5, before=self.term)

        self.prog_exec["maximum"] = len(itens)
        self.prog_exec["value"] = 0
        self.txt_eta.set("Calculando ETA...")
        self.start_time_exec = time.time()

        self.log(f"Iniciando auditoria de {len(itens)} itens...")
        self.app.prevenir_suspensao(True)

        callbacks = {
            "on_status": self.cb_status,
            "on_log": self.log,
            "on_progress": self.cb_progress,
            "ask_duplicate": self.cb_ask_duplicate,
        }

        pular = self.pular_existentes.get()
        threading.Thread(
            target=lambda: self._thread_run(itens, callbacks, pular), daemon=True
        ).start()

    def _thread_run(self, itens, callbacks, pular_flag):
        try:
            self.app.motor.executar_fila(itens, callbacks, pular_existentes=pular_flag)
        except Exception as e:
            import traceback

            traceback.print_exc()
            self.log(f"Erro Fatal na Thread: {e}")
        finally:
            self.app.after(0, self.finalizar_processo)

    def finalizar_processo(self):
        self.app.motor.rodando_event.clear()
        self._update_ui_state(running=False)
        self.app.prevenir_suspensao(False)

        # Oculta a barra de progresso para limpar a tela
        self.f_prog.pack_forget()
        self.prog_exec.pack_forget()

        if hasattr(self.app.logger, "salvar_sessao_csv"):
            try:
                self.app.logger.salvar_sessao_csv()
            except Exception as e:
                print(f"Erro ao salvar log CSV: {e}")

        tempo_total = str(timedelta(seconds=int(time.time() - self.start_time_exec)))
        self.log(f"Fim. Tempo Total: {tempo_total}")
        winsound.MessageBeep(winsound.MB_OK)

        try:
            reporter = AuditReporter()
            path = reporter.gerar_relatorio(
                self.app.logger.sessao_atual,
                tempo_total,
                str(self.app.motor.pasta_downloads),
            )
            if path and os.path.exists(path):
                if messagebox.askyesno(
                    "Finalizado", "Auditoria concluída.\nAbrir relatório HTML?"
                ):
                    os.startfile(path)
        except Exception as e:
            print(f"Erro ao gerar relatório: {e}")

    # ==========================
    # 3. CALLBACKS DO MOTOR
    # ==========================
    def cb_status(self, iid, status, tags, tempo=None, linhas=None):
        self.app.after(
            0, lambda: self._update_tree_row(iid, status, tags, tempo, linhas)
        )

    def _update_tree_row(self, iid, status, tags, tempo, linhas):
        try:
            self.tree.set(iid, "Status", status)
            if tempo:
                self.tree.set(iid, "Tempo_DL", tempo)
            if linhas:
                self.tree.set(iid, "Linhas", linhas)

            if tags:
                self.tree.item(iid, tags=tags)
            self.tree.see(iid)

            status_finais = [
                "Concluído",
                "Erro",
                "Vazio",
                "Pulado (Existe)",
                "Timeout DL",
                "Falha Clique",
                "Erro Mover",
                "Erro Link",
            ]
            if status in status_finais:
                try:
                    item = self.lista_exec[int(iid)]
                    qtd_linhas = linhas if linhas else "-"
                    if status == "Concluído" and (not qtd_linhas or qtd_linhas == "-"):
                        try:
                            mes_atual = datetime.now().strftime("%m.%Y")
                            caminho_final = os.path.join(
                                item["path"], mes_atual, f"{item['name']}.csv"
                            )
                            if os.path.exists(caminho_final):
                                qtd = self.app.data_processor.contar_linhas(
                                    caminho_final
                                )
                                qtd_linhas = str(qtd)
                                self.tree.set(iid, "Linhas", qtd_linhas)
                        except:
                            pass

                    self.app.logger.registrar(
                        item["name"], status, qtd_linhas, item["link"], tempo=tempo
                    )
                except Exception as e:
                    print(f"Erro ao registrar log UI: {e}")
        except Exception as e:
            print(f"Erro ao atualizar UI: {e}")

    def cb_progress(self, val):
        self.app.after(0, lambda: self._update_prog_bar(val))

    def _update_prog_bar(self, val):
        self.prog_exec["value"] = val
        if val > 0:
            elapsed = time.time() - self.start_time_exec
            total = self.prog_exec["maximum"]
            if total > 0 and val <= total:
                rate = elapsed / val
                eta = int(rate * (total - val))
                self.txt_eta.set(f"Tempo Restante Aprox: {str(timedelta(seconds=eta))}")

    def cb_ask_duplicate(self, filepath):
        event = threading.Event()
        self.decisao_usuario.set("")
        self.app.after(0, lambda: self._popup_duplicate(filepath, event))
        event.wait()
        return self.decisao_usuario.get()

    def _popup_duplicate(self, filepath, event):
        top = tk.Toplevel(self.app)
        top.title("Conflito Detectado")
        top.geometry("450x220")
        top.configure(bg=self.app.colors["panel"])
        top.attributes("-topmost", True)
        top.protocol(
            "WM_DELETE_WINDOW",
            lambda: [self.decisao_usuario.set("copia"), event.set(), top.destroy()],
        )

        tk.Label(
            top,
            text="⚠ Arquivo já existe",
            font=("Segoe UI", 12, "bold"),
            bg=self.app.colors["panel"],
            fg=self.app.colors["warning"],
        ).pack(pady=15)

        tk.Label(
            top,
            text=os.path.basename(filepath),
            bg=self.app.colors["panel"],
            fg=self.app.colors["fg"],
        ).pack()

        def reply(choice):
            self.decisao_usuario.set(choice)
            event.set()
            top.destroy()

        btn_fr = tk.Frame(top, bg=self.app.colors["panel"])
        btn_fr.pack(pady=25)
        tk.Button(
            btn_fr,
            text="Substituir",
            bg=self.app.colors["danger"],
            fg="white",
            width=15,
            command=lambda: reply("substituir"),
        ).pack(side="left", padx=15)
        tk.Button(
            btn_fr,
            text="Criar Cópia",
            bg=self.app.colors["accent"],
            fg="white",
            width=15,
            command=lambda: reply("copia"),
        ).pack(side="left", padx=15)

        self.app.after(10000, lambda: reply("copia") if top.winfo_exists() else None)

    # ==========================
    # 4. FUNÇÕES DE SUPORTE E UI
    # ==========================
    def log(self, msg):
        ts = datetime.now().strftime("%H:%M:%S")

        def _l():
            if hasattr(self, "term"):
                self.term.config(state="normal")
                self.term.insert("end", f"[{ts}] {msg}\n")
                self.term.see("end")
                self.term.config(state="disabled")

        self.app.after(0, _l)

    def _update_ui_state(self, running):
        s_start = "disabled" if running else "normal"
        s_stop = "normal" if running else "disabled"
        if hasattr(self, "btn_iniciar"):
            self.btn_iniciar.config(state=s_start)
            self.btn_parar.config(state=s_stop)
            self.btn_pausar.config(state=s_stop, text="⏸ PAUSAR")

    def toggle_pause(self):
        if not self.app.motor.rodando_event.is_set():
            return
        if not hasattr(self.app.motor, "toggle_pause"):
            return

        self.app.motor.toggle_pause()
        is_paused = self.app.motor.pausado_event.is_set()
        self.btn_pausar.config(text="▶ RETOMAR" if is_paused else "⏸ PAUSAR")
        self.log("⏸ Pausado..." if is_paused else "▶ Retomado.")

    def _confirmar_parada(self):
        if messagebox.askyesno(
            "Confirmar Parada",
            "Tem certeza que deseja interromper todas as operações? \nIsso pode deixar arquivos incompletos.",
        ):
            self.parar()

    def parar(self):
        self.app.motor.rodando_event.clear()
        self.app.motor.pausado_event.clear()
        if hasattr(self.app.motor, "driver") and self.app.motor.driver:
            try:
                self.app.motor.driver.quit()
            except:
                pass
        self.log("⏹ Parada Iniciada...")

    # ==========================
    # 5. GERENCIAMENTO DA TABELA
    # ==========================
    def scan_links(self):
        self.tree.delete(*self.tree.get_children())
        self.lista_exec = []
        path = self.app.path_rede.get()
        if not os.path.exists(path):
            return messagebox.showwarning("Ops", "Selecione uma pasta válida.")

        pular_existentes_flag = self.pular_existentes.get()
        self.txt_status_busca.set("Escaneando...")

        # Exibe a barra de carregamento
        self.prog_scan.pack(fill="x", pady=(5, 0), before=self.term)
        self.prog_scan.start(10)
        self.log(f"Varredura iniciada em: {path}")

        def _scan():
            count = 0
            links_vistos = set()
            batch = []
            try:
                for root, _, files in os.walk(path):
                    for f in files:
                        if f.lower().endswith(".txt") and "vazio" not in f.lower():
                            full = os.path.join(root, f)
                            try:
                                links = []
                                try:
                                    with open(full, "r", encoding="utf-8") as fi:
                                        for linha in fi:
                                            links.extend(
                                                re.findall(r"https?://[^\s]+", linha)
                                            )
                                except UnicodeDecodeError:
                                    with open(full, "r", encoding="latin-1") as fi:
                                        for linha in fi:
                                            links.extend(
                                                re.findall(r"https?://[^\s]+", linha)
                                            )

                                if not links:
                                    continue

                                base = os.path.splitext(f)[0]
                                base = re.sub(
                                    r"(?i)^(relat[oó]rio|report|robo|audit)\s*[-_]?\s*",
                                    "",
                                    base,
                                ).strip()

                                for i, lnk in enumerate(links):
                                    if lnk in links_vistos:
                                        continue
                                    links_vistos.add(lnk)

                                    name = f"{base}" + (
                                        f"_pt{i+1}" if len(links) > 1 else ""
                                    )
                                    status = (
                                        "Feito"
                                        if self.app.logger.verificar_ja_feito(lnk)
                                        else "Pendente"
                                    )

                                    if pular_existentes_flag and status == "Feito":
                                        continue

                                    batch.append((name, root, lnk, status))
                                    count += 1

                                    if len(batch) >= 50:
                                        self.app.after_idle(
                                            lambda b=batch.copy(): [
                                                self._add_item(*args) for args in b
                                            ]
                                        )
                                        batch.clear()
                            except Exception as e:
                                print(
                                    f"Aviso: Falha de I/O ou Permissão ao ler arquivo {f}: {e}"
                                )
            finally:
                if batch:
                    self.app.after(
                        0, lambda b=batch.copy(): [self._add_item(*args) for args in b]
                    )
                self.app.after(
                    0,
                    lambda: [
                        self.txt_total_encontrados.set(f"{count} Links"),
                        self.prog_scan.stop(),
                        self.prog_scan.pack_forget(),  # Oculta a barra ao terminar
                        self.txt_status_busca.set("Pronto"),
                    ],
                )

        threading.Thread(target=_scan, daemon=True).start()

    def _add_item(self, n, p, l, s):
        mes_atual = datetime.now().strftime("%m.%Y")
        status_existencia_str = "-"

        if hasattr(self.app.motor, "verificar_existencia_dados"):
            try:
                status_dados = self.app.motor.verificar_existencia_dados(
                    n, p, mes_atual
                )
                if status_dados["existe_csv"]:
                    status_existencia_str = "✅ Dados Existem"
                elif status_dados["existe_vazio"]:
                    status_existencia_str = "⚠ Arquivo Vazio"
                else:
                    status_existencia_str = "❌ Não Encontrado"
            except Exception as e:
                print(f"Erro ao verificar existência UI: {e}")

        tag = "ok" if s == "Feito" else ""
        iid = len(self.lista_exec)

        self.lista_exec.append(
            {
                "iid": iid,
                "name": n,
                "path": p,
                "link": l,
                "sel": True,
                "status_existencia": status_existencia_str,
            }
        )

        self.tree.insert(
            "",
            "end",
            iid=str(iid),
            values=("[x]", n, s, status_existencia_str, "-", "-"),
            tags=(tag,),
        )

    def atualizar_existencia_em_lote(self):
        if not self.lista_exec:
            return messagebox.showinfo(
                "Info", "Nenhum item carregado para atualização."
            )

        mes_atual = datetime.now().strftime("%m.%Y")
        snapshot_lista = list(self.lista_exec)

        def _thread_atualizacao():
            for idx, item in enumerate(snapshot_lista):
                if not self.app.motor.rodando_event.is_set():
                    try:
                        status = self.app.motor.verificar_existencia_dados(
                            item["name"], item["path"], mes_atual
                        )
                        status_str = self._formatar_status_existencia(status)

                        def update_ui_safe(i=idx, s=status_str, ref_item=item):
                            if (
                                i < len(self.lista_exec)
                                and self.lista_exec[i] is ref_item
                            ):
                                self.tree.set(i, "Existência", s)
                                self.lista_exec[i].update({"status_existencia": s})

                        self.app.after(0, update_ui_safe)
                    except Exception as e:
                        print(f"Erro ao atualizar item {item['name']}: {e}")

            self.app.after(0, lambda: self.log("✅ Existência em lote atualizada"))

        threading.Thread(target=_thread_atualizacao, daemon=True).start()

    def _formatar_status_existencia(self, status):
        if status["existe_csv"]:
            return "✅ Dados Existem"
        elif status["existe_vazio"]:
            return "⚠ Arquivo Vazio"
        else:
            return "❌ Não Encontrado"

    def on_tree_click_single(self, e):
        r = self.tree.identify("region", e.x, e.y)
        if r == "cell" and self.tree.identify_column(e.x) == "#1":
            iid = self.tree.identify_row(e.y)
            if iid:
                idx = int(iid)
                new = not self.lista_exec[idx]["sel"]
                self.lista_exec[idx]["sel"] = new
                self.tree.set(iid, "Sel", "[x]" if new else "[ ]")

    def batch_select_all(self, val, only_highlighted=False):
        if not self.lista_exec:
            return
        selecionados_gui = self.tree.selection()
        target_ids = (
            [int(iid) for iid in selecionados_gui]
            if only_highlighted and selecionados_gui
            else [item["iid"] for item in self.lista_exec]
        )
        char = "[x]" if val else "[ ]"
        for idx in target_ids:
            if idx < len(self.lista_exec):
                self.lista_exec[idx]["sel"] = val
                if self.tree.exists(idx):
                    self.tree.set(idx, "Sel", char)

    def abrir_menu_contexto(self, e):
        try:
            self.menu_contexto.tk_popup(e.x_root, e.y_root)
        finally:
            self.menu_contexto.grab_release()

    def abrir_link_selecionado(self):
        sel = self.tree.selection()
        if not sel:
            return
        try:
            idx = int(sel[0])
            item = self.lista_exec[idx]
            link = item.get("link")
            if link:
                webbrowser.open(link)
                self.log(f"Abrindo link: {item['name']}")
        except Exception as e:
            self.log(f"Erro ao abrir link selecionado: {e}")
