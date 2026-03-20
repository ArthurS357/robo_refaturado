import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import threading
import os
import re
import glob
import json
import csv
from pathlib import Path

# --- Importação Opcional: Pandas ---
try:
    import pandas as pd

    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

from tab_base import BaseTab
from audit_utils import MaintenanceTool


class TabFerramentas(BaseTab):
    DEFAULT_NETWORK_PATH = r"\\bsbrsp56\AuditTM\TECNOLOGIA\AUDITORIAS\2026\Digitalizações\Digitalização - Gestão de Ativos\Digitalização (CMDB)"

    def __init__(self, parent, app):
        super().__init__(parent, app)

        # --- Variáveis Locais da Aba ---
        self.path_unificacao_var = tk.StringVar(value="")
        self.mes_selecionado = tk.StringVar()

    # ==========================
    # 1. CONSTRUÇÃO DA INTERFACE
    # ==========================
    def _criar_card_ferramenta(self, parent, titulo, icone_emoji):
        """Helper para criar os cartões de ferramenta."""
        card = ttk.Frame(parent, style="ToolCard.TFrame", padding=20)
        card.pack(fill="x", pady=10)

        f_icon = ttk.Frame(card, style="ToolCard.TFrame")
        f_icon.pack(side="left", padx=(0, 20), anchor="n")
        ttk.Label(f_icon, text=icone_emoji, style="ToolIcon.TLabel").pack()

        f_content = ttk.Frame(card, style="ToolCard.TFrame")
        f_content.pack(side="left", fill="both", expand=True)
        ttk.Label(f_content, text=titulo, style="ToolTitle.TLabel").pack(
            anchor="w", pady=(0, 5)
        )

        return f_content

    def montar(self):
        container = self._criar_area_rolavel(self.parent)
        content_frame = ttk.Frame(container, padding=20)
        content_frame.pack(fill="both", expand=True)

        # --- 1. Unificação ---
        c_unif = self._criar_card_ferramenta(
            content_frame, "Unificação de Partes (.csv)", "🔗"
        )
        ttk.Label(
            c_unif,
            text="Selecione a pasta onde estão os arquivos divididos (_pt1, _pt2...). Será gerado um arquivo UNIFICADO_ para cada grupo encontrado.",
            style="ToolCard.TLabel",
            wraplength=800,
        ).pack(anchor="w")

        f_inp = ttk.Frame(c_unif, style="ToolCard.TFrame")
        f_inp.pack(fill="x", pady=10)
        ttk.Entry(
            f_inp, textvariable=self.path_unificacao_var, font=self.app.font_body
        ).pack(side="left", fill="x", expand=True, padx=(0, 5))
        ttk.Button(
            f_inp,
            text="📁",
            width=4,
            command=lambda: self.path_unificacao_var.set(filedialog.askdirectory()),
        ).pack(side="left")

        self.prog_unif = ttk.Progressbar(c_unif, mode="indeterminate")
        self.btn_unif = ttk.Button(
            c_unif, text="Executar Unificação", command=self.tool_unificar_pasta
        )
        self.btn_unif.pack(anchor="e")

        # --- 2. Master ---
        c_mast = self._criar_card_ferramenta(
            content_frame, "Gerador de MASTER Mensal", "📊"
        )
        ttk.Label(
            c_mast,
            text=f"Origem Rede: {self.DEFAULT_NETWORK_PATH[:40]}...",
            font=("Segoe UI", 8, "italic"),
            style="ToolCard.TLabel",
        ).pack(anchor="w", pady=(0, 5))
        ttk.Label(
            c_mast,
            text="Selecione o mês abaixo. O sistema buscará recursivamente todas as pastas com esse mês e consolidará os CSVs em um único arquivo MASTER.",
            style="ToolCard.TLabel",
            wraplength=800,
        ).pack(anchor="w")

        f_m = ttk.Frame(c_mast, style="ToolCard.TFrame")
        f_m.pack(fill="x", pady=10)
        ttk.Label(f_m, text="Mês Alvo (MM.YYYY):", style="ToolCard.TLabel").pack(
            side="left"
        )

        self.cb_meses = ttk.Combobox(
            f_m,
            textvariable=self.mes_selecionado,
            width=20,
            font=self.app.font_body,
            state="readonly",
        )
        self.cb_meses.pack(side="left", padx=10)
        self.cb_meses.bind("<Button-1>", lambda e: self.atualizar_lista_meses_rede())

        self.prog_master = ttk.Progressbar(c_mast, mode="indeterminate")
        self.btn_master = ttk.Button(
            c_mast, text="Gerar Arquivo Master", command=self.tool_gerar_master
        )
        self.btn_master.pack(anchor="e")

        # --- 3. Validação CMDB vs Master ---
        c_val = self._criar_card_ferramenta(
            content_frame, "Validação CMDB vs Master", "✅"
        )
        ttk.Label(
            c_val,
            text=(
                "Cruza os campos mandatórios do Master (Mandatory) com as colunas da base CMDB "
                "e gera um relatório Excel com CHECK = ✅ Presente / ❌ Ausente por campo e classe."
            ),
            style="ToolCard.TLabel",
            wraplength=800,
        ).pack(anchor="w", pady=(0, 10))

        self.prog_val = ttk.Progressbar(c_val, mode="indeterminate")
        self.btn_val = ttk.Button(
            c_val,
            text="Executar Validação Interativa",
            style="Primary.TButton",
            command=self.tool_validar_cmdb,
        )
        self.btn_val.pack(anchor="e")

        # --- 3.5 Relatório de Completude CMDB ---
        c_comp = self._criar_card_ferramenta(
            content_frame, "Relatório de Completude CMDB", "📈"
        )
        ttk.Label(
            c_comp,
            text="Analisa a base gigante do CMDB e cruza com os campos obrigatórios do Master, gerando um Excel com % de preenchimento.",
            style="ToolCard.TLabel",
            wraplength=800,
        ).pack(anchor="w", pady=(0, 10))

        self.prog_comp = ttk.Progressbar(c_comp, mode="indeterminate")
        self.btn_comp = ttk.Button(
            c_comp,
            text="Gerar Análise de Completude",
            style="Primary.TButton",
            command=self.tool_completude_cmdb,
        )
        self.btn_comp.pack(anchor="e")

        # --- 4. Manutenção ---
        c_maint = self._criar_card_ferramenta(
            content_frame, "Manutenção e Integridade", "🛡️"
        )
        self.f_act = ttk.Frame(c_maint, style="ToolCard.TFrame")
        self.f_act.pack(fill="x", pady=10)

        self.prog_maint = ttk.Progressbar(c_maint, mode="indeterminate")
        ttk.Button(
            self.f_act,
            text="🧹 Limpar Logs Antigos (>7 dias)",
            command=self._limpar_logs_seguro,
        ).pack(side="left", padx=5)

        # --- 5. Parquet ---
        c_parq = self._criar_card_ferramenta(
            content_frame, "Conversor CSV / Excel → Parquet", "📦"
        )
        ttk.Label(
            c_parq,
            text=(
                "Selecione um ou mais arquivos CSV ou Excel para consolidar "
                "em um único arquivo .parquet otimizado."
            ),
            style="ToolCard.TLabel",
            wraplength=800,
        ).pack(anchor="w", pady=(0, 10))

        self.prog_parquet = ttk.Progressbar(c_parq, mode="indeterminate")
        self.btn_parq = ttk.Button(
            c_parq,
            text="Executar Conversor Interativo",
            style="Primary.TButton",
            command=self.tool_converter_parquet,
        )
        self.btn_parq.pack(anchor="e")

        # --- 6. Análise de Existência ---
        c_analise = self._criar_card_ferramenta(
            content_frame, "Análise de Existência de Dados", "📋"
        )
        ttk.Label(
            c_analise,
            text="Analisa sistematicamente onde os dados não estão sendo gerados corretamente.",
            style="ToolCard.TLabel",
            wraplength=800,
        ).pack(anchor="w", pady=(0, 10))

        self.prog_analise = ttk.Progressbar(c_analise, mode="indeterminate")
        self.btn_analise = ttk.Button(
            c_analise,
            text="Executar Análise de Problemas",
            command=self.analisar_problemas_existencia,
        )
        self.btn_analise.pack(anchor="e")

    # ==========================
    # 2. LÓGICA DAS FERRAMENTAS
    # ==========================

    # ── UNIFICAÇÃO DE PARTES ─────────────────────────────────────────────────────
    def tool_unificar_pasta(self):
        """
        Unifica arquivos _pt1, _pt2... numa pasta selecionada.
        Valida o caminho antes de iniciar e exibe resultado detalhado ao final.
        """
        d = self.path_unificacao_var.get().strip()

        if not d:
            return messagebox.showwarning(
                "Pasta não selecionada",
                "Clique em 📁 para selecionar a pasta que contém os arquivos a unificar.",
            )
        if not os.path.exists(d) or not os.path.isdir(d):
            return messagebox.showerror(
                "Caminho inválido",
                f"A pasta abaixo não existe ou está inacessível:\n\n{d}",
            )

        self.btn_unif.config(state="disabled")
        self.prog_unif.pack(fill="x", pady=5, before=self.btn_unif)
        self.prog_unif.start(10)
        self.app.log(f"Iniciando unificação em: {d}")

        def _thread_unif():
            try:
                resultado = self.app.motor.processar_fusao_partes(d)

                # processar_fusao_partes retorna o dict de unificar_partes
                if isinstance(resultado, dict):
                    unificados = resultado.get("unificados", 0)
                    msg_backend = resultado.get("msg", "Processo finalizado.")
                    erros = resultado.get("erros", [])

                    if unificados == 0:
                        # Sem partes encontradas OU sem grupos com 2+ partes
                        self.app.after(
                            0,
                            lambda: messagebox.showinfo(
                                "Nada a unificar",
                                f"{msg_backend}\n\n"
                                "Certifique-se de que os arquivos possuem sufixo _pt1, _pt2... no nome.",
                            ),
                        )
                    else:
                        detalhe_erros = (
                            f"\n\n⚠️ Erros encontrados ({len(erros)}):\n"
                            + "\n".join(erros[:5])
                            if erros
                            else ""
                        )
                        self.app.after(
                            0,
                            lambda: messagebox.showinfo(
                                "Unificação Concluída ✅",
                                f"{msg_backend}\n\n"
                                f"Os arquivos UNIFICADO_*.csv foram salvos em:\n{d}"
                                f"{detalhe_erros}",
                            ),
                        )
                        self.app.after(0, lambda: os.startfile(d))
                else:
                    # Fallback: backend antigo sem dict (segurança)
                    self.app.after(
                        0,
                        lambda: messagebox.showinfo(
                            "Concluído", "Unificação finalizada."
                        ),
                    )

            except Exception as e:
                import traceback

                tb = traceback.format_exc()
                self.app.after(
                    0,
                    lambda: messagebox.showerror(
                        "Erro na Unificação",
                        f"Ocorreu um erro inesperado:\n\n{e}\n\n{tb}",
                    ),
                )
            finally:
                self.app.after(
                    0,
                    lambda: [
                        self.prog_unif.stop(),
                        self.prog_unif.pack_forget(),
                        self.btn_unif.config(state="normal"),
                    ],
                )

        threading.Thread(target=_thread_unif, daemon=True).start()

    # ── GERADOR DE MASTER MENSAL ─────────────────────────────────────────────────
    def atualizar_lista_meses_rede(self):
        path_base = self.DEFAULT_NETWORK_PATH
        if not os.path.exists(path_base):
            current = self.app.path_rede.get()
            if current and os.path.exists(current):
                path_base = current
            else:
                if messagebox.askyesno(
                    "Rede", "Caminho padrão não encontrado. Selecionar manualmente?"
                ):
                    d = filedialog.askdirectory()
                    if d:
                        path_base = d
                    else:
                        return
                else:
                    return

        self.app.path_rede.set(path_base)
        self.cb_meses.set("Buscando...")
        self.app.update()

        def _search_thread():
            try:
                meses_encontrados = set()
                nivel_max = path_base.count(os.sep) + 3
                for root, dirs, _ in os.walk(path_base):
                    if root.count(os.sep) > nivel_max:
                        continue
                    for d in dirs:
                        if re.match(r"^\d{2}\.\d{4}$", d):
                            meses_encontrados.add(d)
                lista = sorted(list(meses_encontrados), reverse=True)
                self.app.after(
                    0,
                    lambda: [
                        self.cb_meses.config(values=lista),
                        self.cb_meses.current(0) if lista else None,
                    ],
                )
            except Exception:
                self.app.after(0, lambda: self.cb_meses.set("Erro"))

        threading.Thread(target=_search_thread, daemon=True).start()

    def tool_gerar_master(self):
        """
        Gera o arquivo Master consolidado para o mês selecionado.
        Varre recursivamente a pasta de rede buscando todas as subpastas com o mês.
        """
        m = self.mes_selecionado.get().strip()
        r = self.app.path_rede.get().strip()

        if not m:
            return messagebox.showwarning(
                "Aviso", "Selecione um mês no combobox antes de continuar."
            )
        if not r:
            return messagebox.showwarning(
                "Aviso", "Selecione a Pasta de Rede na aba Execução."
            )
        if not os.path.exists(r):
            return messagebox.showerror(
                "Pasta inacessível",
                f"A pasta abaixo não foi encontrada ou está offline:\n\n{r}",
            )
        if not re.match(r"^\d{2}\.\d{4}$", m):
            return messagebox.showwarning(
                "Formato inválido",
                f"O mês deve estar no formato MM.YYYY (ex: 03.2026).\nValor informado: '{m}'",
            )

        self.btn_master.config(state="disabled")
        self.prog_master.pack(fill="x", pady=5, before=self.btn_master)
        self.prog_master.start(10)
        self.app.log(f"Gerando MASTER para {m} a partir de: {r}")

        def _thread_master():
            try:
                output_path = self.app.motor.processar_master(r, m)

                if output_path and os.path.exists(output_path):
                    tamanho_mb = os.path.getsize(output_path) / (1024 * 1024)
                    msg = (
                        f"✅ MASTER gerado com sucesso!\n\n"
                        f"Arquivo:  {os.path.basename(output_path)}\n"
                        f"Tamanho:  {tamanho_mb:.2f} MB\n"
                        f"Local:    {output_path}"
                    )
                    self.app.after(
                        0, lambda: messagebox.showinfo("Master Concluído", msg)
                    )
                    self.app.after(
                        0, lambda: os.startfile(os.path.dirname(output_path))
                    )
                else:
                    self.app.after(
                        0,
                        lambda: messagebox.showwarning(
                            "Nenhum dado encontrado",
                            f"Não foram encontrados arquivos CSV válidos em pastas "
                            f"nomeadas '{m}' dentro de:\n\n{r}\n\n"
                            "Verifique se o mês selecionado está correto e se a pasta de rede está acessível.",
                        ),
                    )
            except Exception as e:
                import traceback

                tb = traceback.format_exc()
                self.app.after(
                    0,
                    lambda: messagebox.showerror(
                        "Erro ao Gerar Master", f"Ocorreu um erro:\n\n{e}\n\n{tb}"
                    ),
                )
            finally:
                self.app.after(
                    0,
                    lambda: [
                        self.prog_master.stop(),
                        self.prog_master.pack_forget(),
                        self.btn_master.config(state="normal"),
                    ],
                )

        threading.Thread(target=_thread_master, daemon=True).start()

    # ── VALIDAÇÃO CMDB vs MASTER ─────────────────────────────────────────────────
    def tool_validar_cmdb(self):
        """
        Valida os campos mandatórios do Master contra as colunas presentes na base CMDB.
        Usa StructureValidator.executar_validacao_completa (adicionado em audit_data.py).
        """
        if not HAS_PANDAS:
            return messagebox.showerror(
                "Dependência faltando",
                "A biblioteca 'pandas' não está instalada.\n"
                "Execute no terminal:\n\n  pip install pandas openpyxl",
            )

        cmdb_path = filedialog.askopenfilename(
            title="Passo 1/3 — Selecione a Base CMDB (CSV ou Excel)",
            filetypes=[("Base de Dados", "*.csv *.xlsx *.xls"), ("Todos", "*.*")],
        )
        if not cmdb_path:
            return

        master_path = filedialog.askopenfilename(
            title="Passo 2/3 — Selecione o Modelo Master (Excel)",
            filetypes=[("Arquivos Excel", "*.xlsx *.xls")],
        )
        if not master_path:
            return

        output_path = filedialog.asksaveasfilename(
            title="Passo 3/3 — Onde salvar o Relatório de Validação?",
            defaultextension=".xlsx",
            filetypes=[("Excel", "*.xlsx")],
            initialfile="Validacao_CMDB_vs_Master.xlsx",
        )
        if not output_path:
            return

        self.app.log("Iniciando validação CMDB vs Master... Aguarde.")
        self.btn_val.config(state="disabled")
        self.prog_val.pack(fill="x", pady=5, before=self.btn_val)
        self.prog_val.start(10)

        def _thread_validar():
            try:
                from audit_data import StructureValidator

                validador = StructureValidator()
                sucesso, mensagem = validador.executar_validacao_completa(
                    cmdb_path, master_path, output_path
                )

                if sucesso:
                    self.app.after(
                        0,
                        lambda: messagebox.showinfo("Validação Concluída ✅", mensagem),
                    )
                    self.app.after(0, lambda: os.startfile(output_path))
                else:
                    self.app.after(
                        0, lambda: messagebox.showerror("Falha na Validação", mensagem)
                    )
            except Exception as e:
                import traceback

                tb = traceback.format_exc()
                self.app.after(
                    0,
                    lambda: messagebox.showerror(
                        "Erro Crítico na Validação", f"{e}\n\n{tb}"
                    ),
                )
            finally:
                self.app.after(
                    0,
                    lambda: [
                        self.prog_val.stop(),
                        self.prog_val.pack_forget(),
                        self.btn_val.config(state="normal"),
                    ],
                )

        threading.Thread(target=_thread_validar, daemon=True).start()

    # ── COMPLETUDE CMDB ──────────────────────────────────────────────────────────
    def tool_completude_cmdb(self):
        cmdb_path = filedialog.askopenfilename(
            title="Passo 1/3: Selecione a Base CMDB (CSV)", filetypes=[("CSV", "*.csv")]
        )
        if not cmdb_path:
            return

        master_path = filedialog.askopenfilename(
            title="Passo 2/3: Selecione o Modelo Master (Excel)",
            filetypes=[("Excel", "*.xlsx *.xls")],
        )
        if not master_path:
            return

        output_path = filedialog.asksaveasfilename(
            title="Passo 3/3: Onde salvar o Relatório Final?",
            defaultextension=".xlsx",
            filetypes=[("Excel", "*.xlsx")],
        )
        if not output_path:
            return

        self.app.log(
            "Iniciando a geração do Relatório de Completude... Isso pode levar alguns minutos."
        )

        self.prog_comp.pack(fill="x", pady=5, before=self.btn_comp)
        self.prog_comp.start(10)

        def _thread_completude():
            try:
                sucesso, mensagem = self.app.data_processor.gerar_relatorio_completude(
                    cmdb_path, master_path, output_path
                )

                if sucesso:
                    self.app.after(
                        0, lambda: messagebox.showinfo("Processo Concluído", mensagem)
                    )
                    self.app.after(0, lambda: os.startfile(output_path))
                else:
                    self.app.after(
                        0, lambda: messagebox.showerror("Erro na Análise", mensagem)
                    )
            except Exception as e:
                self.app.after(
                    0, lambda err=e: messagebox.showerror("Erro Crítico", str(err))
                )
            finally:
                self.app.after(
                    0, lambda: [self.prog_comp.stop(), self.prog_comp.pack_forget()]
                )

        threading.Thread(target=_thread_completude, daemon=True).start()

    # ── MANUTENÇÃO ───────────────────────────────────────────────────────────────
    def _limpar_logs_seguro(self):
        if messagebox.askyesno(
            "Limpeza de Logs",
            "Isso apagará permanentemente logs com mais de 7 dias.\nDeseja continuar?",
        ):
            self.tool_limpeza()

    def tool_limpeza(self):
        self.prog_maint.pack(fill="x", pady=5, before=self.f_act)
        self.prog_maint.start(10)

        def _thread_limpeza():
            try:
                q = MaintenanceTool.limpar_logs_antigos()
                self.app.after(
                    0,
                    lambda: messagebox.showinfo("Limpeza", f"{q} arquivos removidos."),
                )
            finally:
                self.app.after(
                    0, lambda: [self.prog_maint.stop(), self.prog_maint.pack_forget()]
                )

        threading.Thread(target=_thread_limpeza, daemon=True).start()

    # ── CONVERSOR PARQUET ────────────────────────────────────────────────────────
    def tool_converter_parquet(self):
        if not HAS_PANDAS:
            return messagebox.showerror(
                "Erro",
                "Biblioteca 'pandas' não instalada.\n"
                "Instale com: pip install pandas pyarrow openpyxl",
            )

        arquivos_selecionados = filedialog.askopenfilenames(
            title="Passo 1/2: Selecione os arquivos CSV ou Excel para converter",
            filetypes=[
                ("Arquivos Suportados", "*.csv *.xlsx *.xls"),
                ("CSV", "*.csv"),
                ("Excel", "*.xlsx *.xls"),
                ("Todos os Arquivos", "*.*"),
            ],
        )
        if not arquivos_selecionados:
            return

        arquivo_saida = filedialog.asksaveasfilename(
            title="Passo 2/2: Salvar arquivo Parquet como...",
            defaultextension=".parquet",
            filetypes=[("Arquivo Parquet", "*.parquet")],
        )
        if not arquivo_saida:
            return

        self.app.log(
            f"Iniciando conversão Parquet: {len(arquivos_selecionados)} arquivo(s) selecionado(s)"
        )
        self.prog_parquet.pack(fill="x", pady=5, before=self.btn_parq)
        self.prog_parquet.start(10)

        threading.Thread(
            target=lambda: self._thread_parquet_logic(
                list(arquivos_selecionados), arquivo_saida
            ),
            daemon=True,
        ).start()

    def _detectar_separador_csv(self, caminho):
        """Detecta separador e encoding do CSV a partir de uma amostra inicial."""
        import csv as csv_mod

        for enc in ("utf-8-sig", "latin-1", "cp1252"):
            try:
                with open(caminho, "r", encoding=enc) as f:
                    amostra = f.read(16384)
                dialeto = csv_mod.Sniffer().sniff(amostra, delimiters=",;\t|")
                return dialeto.delimiter, enc
            except Exception:
                continue
        return ",", "utf-8-sig"

    def _ler_arquivo_para_df(self, caminho):
        """Lê um CSV ou Excel e retorna um DataFrame padronizado."""
        ext = os.path.splitext(caminho)[1].lower()

        if ext == ".csv":
            separador, encoding = self._detectar_separador_csv(caminho)

            read_kwargs = dict(
                filepath_or_buffer=caminho,
                sep=separador,
                encoding=encoding,
                low_memory=False,
            )

            try:
                df = pd.read_csv(on_bad_lines="warn", **read_kwargs)
            except TypeError:
                df = pd.read_csv(
                    error_bad_lines=False, warn_bad_lines=True, **read_kwargs
                )

        elif ext in (".xlsx", ".xls"):
            df = pd.read_excel(caminho, engine="openpyxl" if ext == ".xlsx" else "xlrd")
        else:
            raise ValueError(f"Formato não suportado: {ext}")

        df.columns = df.columns.astype(str).str.strip()
        df = df.dropna(axis=1, how="all").dropna(axis=0, how="all")
        df["arquivo_origem"] = os.path.basename(caminho)
        return df

    def _thread_parquet_logic(self, lista_arquivos, arquivo_saida):
        try:
            dataframes = []
            total = len(lista_arquivos)
            erros = []

            for i, arquivo in enumerate(lista_arquivos, start=1):
                try:
                    df = self._ler_arquivo_para_df(arquivo)
                    dataframes.append(df)
                    self.app.after(
                        0,
                        lambda idx=i, nome=os.path.basename(arquivo): self.app.log(
                            f"Parquet: [{idx}/{total}] {nome} OK"
                        ),
                    )
                except Exception as e:
                    erros.append(f"{os.path.basename(arquivo)}: {e}")
                    self.app.after(
                        0,
                        lambda nome=os.path.basename(arquivo), err=e: self.app.log(
                            f"Parquet: ERRO em {nome} — {err}"
                        ),
                    )

            if dataframes:
                self.app.after(
                    0, lambda: self.app.log("Consolidando e otimizando dados...")
                )
                df_final = pd.concat(dataframes, ignore_index=True, sort=False)
                df_final.to_parquet(
                    arquivo_saida, engine="pyarrow", index=False, compression="snappy"
                )

                resumo_erros = (
                    f"\n\n⚠️ {len(erros)} arquivo(s) com erro (ignorados)."
                    if erros
                    else ""
                )
                msg = (
                    f"Arquivo criado: {os.path.basename(arquivo_saida)}\n"
                    f"Arquivos processados: {len(dataframes)}/{total}\n"
                    f"Total de linhas: {len(df_final):,}{resumo_erros}"
                )
                self.app.after(
                    0, lambda: messagebox.showinfo("Conversão Concluída", msg)
                )
            else:
                self.app.after(
                    0,
                    lambda: messagebox.showwarning(
                        "Erro",
                        "Nenhum dado válido para converter.\n" + "\n".join(erros[:10]),
                    ),
                )
        except Exception as e:
            self.app.after(
                0, lambda err=e: messagebox.showerror("Erro Fatal", str(err))
            )
        finally:
            self.app.after(
                0, lambda: [self.prog_parquet.stop(), self.prog_parquet.pack_forget()]
            )

    # ── ANÁLISE DE EXISTÊNCIA ────────────────────────────────────────────────────
    def analisar_problemas_existencia(self):
        self.prog_analise.pack(fill="x", pady=5, before=self.btn_analise)
        self.prog_analise.start(10)

        def _thread_analise():
            try:
                metadados_path = (
                    Path(self.app.motor.pasta_downloads)
                    / "audit_metadados_existencia.json"
                )

                if not metadados_path.exists():
                    self.app.after(
                        0,
                        lambda: messagebox.showinfo(
                            "Info",
                            "Nenhum metadado de existência coletado ainda.\nExecute o robô para gerar dados.",
                        ),
                    )
                    return

                with open(metadados_path, "r", encoding="utf-8") as f:
                    metadados = json.load(f)

                problemas = []
                for registro in metadados:
                    if not registro.get("existe_csv") and not registro.get(
                        "existe_vazio"
                    ):
                        problemas.append(
                            {
                                "nome": registro.get("nome"),
                                "link": registro.get("link"),
                                "ultima_verificacao": registro.get("timestamp"),
                                "problema": "Dados nunca foram baixados (Nem CSV, nem Vazio)",
                            }
                        )
                    elif registro.get("existe_vazio"):
                        problemas.append(
                            {
                                "nome": registro.get("nome"),
                                "link": registro.get("link"),
                                "ultima_verificacao": registro.get("timestamp"),
                                "problema": 'Sistema retornou "Vazio" (Sem dados na fonte)',
                            }
                        )
                    elif (
                        registro.get("existe_csv")
                        and registro.get("tamanho_arquivo", 0) < 500
                    ):
                        problemas.append(
                            {
                                "nome": registro.get("nome"),
                                "link": registro.get("link"),
                                "ultima_verificacao": registro.get("timestamp"),
                                "problema": f'Arquivo muito pequeno ({registro.get("tamanho_arquivo")} bytes)',
                            }
                        )

                if problemas:
                    self.app.after(
                        0, lambda: self._exibir_relatorio_problemas(problemas)
                    )
                else:
                    self.app.after(
                        0,
                        lambda: messagebox.showinfo(
                            "Análise",
                            "Não foram encontrados problemas críticos de existência nos metadados recentes.",
                        ),
                    )
            except Exception as e:
                self.app.after(
                    0,
                    lambda err=e: messagebox.showerror(
                        "Erro", f"Erro na análise: {err}"
                    ),
                )
            finally:
                self.app.after(
                    0,
                    lambda: [self.prog_analise.stop(), self.prog_analise.pack_forget()],
                )

        threading.Thread(target=_thread_analise, daemon=True).start()

    def _exibir_relatorio_problemas(self, problemas):
        top = tk.Toplevel(self.app)
        top.title("Relatório de Problemas de Existência")
        top.geometry("900x600")
        top.configure(bg=self.app.colors["panel"])

        main_frame = ttk.Frame(top, padding=10)
        main_frame.pack(fill="both", expand=True)

        ttk.Label(
            main_frame,
            text="🔍 Problemas de Existência Detectados",
            font=("Segoe UI", 14, "bold"),
            foreground=self.app.colors["accent"],
        ).pack(pady=(0, 10))

        tree_frame = ttk.Frame(main_frame)
        tree_frame.pack(fill="both", expand=True)

        cols = ("Nome", "Problema", "Última Verificação", "Link")
        tree = ttk.Treeview(tree_frame, columns=cols, show="headings", height=15)
        tree.heading("Nome", text="Nome")
        tree.column("Nome", width=250)
        tree.heading("Problema", text="Problema Identificado")
        tree.column("Problema", width=300)
        tree.heading("Última Verificação", text="Data")
        tree.column("Última Verificação", width=150)
        tree.heading("Link", text="Link")
        tree.column("Link", width=100)

        scrollbar = ttk.Scrollbar(tree_frame, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=scrollbar.set)
        tree.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        for p in problemas:
            tree.insert(
                "",
                "end",
                values=(
                    p["nome"],
                    p["problema"],
                    p.get("ultima_verificacao", "")[:16].replace("T", " "),
                    p["link"],
                ),
            )

        btn_frame = ttk.Frame(main_frame)
        btn_frame.pack(fill="x", pady=10)

        def exportar():
            f = filedialog.asksaveasfilename(
                defaultextension=".csv",
                filetypes=[("CSV", "*.csv")],
                initialfile="relatorio_problemas.csv",
            )
            if f:
                try:
                    with open(f, "w", encoding="utf-8-sig", newline="") as csvfile:
                        writer = csv.writer(csvfile, delimiter=";")
                        writer.writerow(cols)
                        for item in tree.get_children():
                            writer.writerow(tree.item(item)["values"])
                    messagebox.showinfo("Sucesso", "Exportado com sucesso!")
                except Exception as e:
                    messagebox.showerror("Erro", str(e))

        ttk.Button(btn_frame, text="Exportar CSV", command=exportar).pack(
            side="right", padx=5
        )
        ttk.Button(btn_frame, text="Fechar", command=top.destroy).pack(side="right")
