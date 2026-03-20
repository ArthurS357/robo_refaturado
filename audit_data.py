import os
import re
import gc
import traceback
from datetime import datetime
import pandas as pd
import numpy as np
from pathlib import Path


class DataProcessor:
    def __init__(self):
        pass

    def contar_linhas(self, path):
        """Conta linhas de um arquivo de forma eficiente (sem carregar tudo na RAM)."""
        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                count = sum(1 for _ in f)
                return max(0, count - 1)  # Desconta cabeçalho, evita negativo
        except:
            return 0

    def listar_historico(self, root_path, dias_filtro):
        """
        Varre a base de dados recursivamente para exibir no histórico (Aba 3).
        AGORA AGRUPA ESTRITAMENTE POR MÊS.ANO.
        """
        dados = []
        try:
            limite_data = None
            if dias_filtro and isinstance(dias_filtro, int):
                from datetime import timedelta

                limite_data = datetime.now() - timedelta(days=dias_filtro)

            for root, dirs, files in os.walk(root_path):
                pasta_atual = os.path.basename(root)
                is_mes = re.match(r"^\d{2}\.\d{4}$", pasta_atual)

                for file in files:
                    if file.lower().endswith(".csv") and "master" not in file.lower():
                        full_path = os.path.join(root, file)
                        try:
                            stats = os.stat(full_path)
                            dt_mod = datetime.fromtimestamp(stats.st_mtime)

                            if limite_data and dt_mod < limite_data:
                                continue

                            tag = "old" if (datetime.now() - dt_mod).days > 7 else "new"
                            nome_relatorio = os.path.basename(os.path.dirname(root))

                            if is_mes:
                                grupo_visual = pasta_atual
                                nome_visual = file
                            else:
                                grupo_visual = f"Outros > {pasta_atual}"
                                nome_visual = file

                            qtd_linhas = self.contar_linhas(full_path)

                            dados.append(
                                {
                                    "nome": nome_visual,
                                    "linhas": qtd_linhas,
                                    "data": dt_mod.strftime("%d/%m/%Y %H:%M"),
                                    "tag": tag,
                                    "caminho": full_path,
                                    "pasta_pai": grupo_visual,
                                    "origem_relatorio": nome_relatorio,
                                }
                            )
                        except Exception as e:
                            print(
                                f"Aviso: Falha ao acessar metadados do arquivo '{file}': {e}"
                            )
                            continue
        except Exception as e:
            print(f"Erro scan histórico: {e}")

        dados.sort(
            key=lambda x: (
                x["pasta_pai"],
                datetime.strptime(x["data"], "%d/%m/%Y %H:%M"),
            ),
            reverse=True,
        )

        return dados, len(dados)

    def verificar_pendencias(self, root_path, mes_ano_alvo, lista_esperada_nomes):
        """
        Procura TODAS as pastas com nome 'MM.YYYY' dentro do root_path
        e consolida os arquivos encontrados para validar pendências.
        """
        arquivos_na_pasta = set()
        pastas_encontradas = []
        print(f"Buscando pastas '{mes_ano_alvo}' em toda a árvore...")

        for root, dirs, files in os.walk(root_path):
            nome_pasta = os.path.basename(root)

            if nome_pasta == mes_ano_alvo:
                pastas_encontradas.append(root)
                for f in files:
                    if f.lower().endswith(".csv"):
                        nome_sem_ext = os.path.splitext(f)[0]
                        nome_limpo = re.sub(
                            r"_Copia_\d+", "", nome_sem_ext, flags=re.IGNORECASE
                        )
                        arquivos_na_pasta.add(nome_limpo.lower())
                        arquivos_na_pasta.add(nome_sem_ext.lower())

        caminho_display = (
            f"{len(pastas_encontradas)} pastas '{mes_ano_alvo}' encontradas."
        )
        if not pastas_encontradas:
            return (
                lista_esperada_nomes,
                "Nenhuma pasta com este mês encontrada na rede.",
            )

        faltantes = []
        for nome in lista_esperada_nomes:
            if nome.lower() not in arquivos_na_pasta:
                faltantes.append(nome)
        return faltantes, caminho_display

    def verificar_integridade(self, path):
        """Tenta ler os primeiros registros para ver se o CSV não está corrompido."""
        bad_files = []
        for root, _, files in os.walk(path):
            for f in files:
                if f.lower().endswith(".csv"):
                    full = os.path.join(root, f)
                    try:
                        self._tenta_ler_csv(full, nrows=5)
                    except Exception as e:
                        bad_files.append((f, str(e)))
        return bad_files

    # --- HELPER PRIVADO: Leitor Robusto ---
    def _tenta_ler_csv(self, filepath, nrows=None):
        """
        Tenta ler um CSV testando combinações de separadores e encodings.
        Retorna um DataFrame ou lança exceção se falhar em todas.
        """
        tentativas = [
            {"enc": "utf-8", "sep": ";"},
            {"enc": "latin-1", "sep": ";"},
            {"enc": "utf-8-sig", "sep": ";"},
            {"enc": "utf-8", "sep": ","},
            {"enc": "latin-1", "sep": ","},
        ]

        last_exception = None

        for t in tentativas:
            try:
                df = pd.read_csv(
                    filepath,
                    encoding=t["enc"],
                    sep=t["sep"],
                    nrows=nrows,
                    on_bad_lines="skip",
                    engine="python",
                )
                if df.shape[1] > 1:
                    return df
            except Exception as e:
                last_exception = e
                continue

        if last_exception:
            raise last_exception
        raise ValueError("Formato CSV não reconhecido (falha na separação de colunas).")

    # --- FUNÇÃO PRINCIPAL DO MASTER ---
    def gerar_master(self, root_path, mes_alvo, output_path=None):
        """
        Varre recursivamente a partir de 'root_path'.
        Agrupa todos os arquivos de pastas com nome 'mes_alvo' num único Master.
        Modo Streaming: baixo consumo de RAM, seguro para milhões de linhas.
        """
        arquivos_para_unir = []

        print(f"Iniciando Gerador Master para mês: {mes_alvo}")
        print(f"Raiz da busca: {root_path}")

        for root, dirs, files in os.walk(root_path):
            nome_pasta = os.path.basename(root)

            if nome_pasta == mes_alvo:
                print(f"-> Coletando de: {root}")
                for file in files:
                    if (
                        file.lower().endswith(".csv")
                        and "master" not in file.lower()
                        and not file.startswith("~$")
                    ):
                        full_p = os.path.join(root, file)
                        arquivos_para_unir.append(full_p)

        if not arquivos_para_unir:
            print(f"Aviso: Nenhuma pasta '{mes_alvo}' ou nenhum CSV encontrado.")
            return None

        print(f"Total de arquivos para unir: {len(arquivos_para_unir)}")

        try:
            if not output_path:
                nome_arquivo = f"MASTER_CONSOLIDADO_{mes_alvo.replace('.','_')}.csv"
                output_path = os.path.join(root_path, nome_arquivo)

            if os.path.exists(output_path):
                os.remove(output_path)

            escreveu_algo = False
            total_linhas = 0
            total_arquivos_ok = 0
            erros = []

            for arq in arquivos_para_unir:
                try:
                    df = self._tenta_ler_csv(arq)

                    if df is None or df.empty:
                        print(f"Arquivo vazio ignorado: {os.path.basename(arq)}")
                        continue

                    df["Origem_Arquivo"] = os.path.basename(arq)
                    df["Origem_Relatorio"] = os.path.basename(os.path.dirname(arq))

                    df.to_csv(
                        output_path,
                        mode="a",
                        header=not escreveu_algo,
                        index=False,
                        encoding="utf-8-sig",
                    )

                    escreveu_algo = True
                    total_linhas += len(df)
                    total_arquivos_ok += 1

                    print(f"OK: {os.path.basename(arq)} ({len(df)} linhas)")

                except Exception as e:
                    msg = f"Erro ao ler '{os.path.basename(arq)}': {e}"
                    print(msg)
                    erros.append(msg)
                    continue

            if escreveu_algo:
                print(
                    f"\nMASTER gerado: {output_path} | {total_arquivos_ok} arquivos | {total_linhas:,} linhas"
                )
                return output_path
            else:
                print("Nenhum dado válido encontrado.")
                return None

        except Exception as e:
            print(f"Erro crítico ao gerar MASTER: {e}")
            return None

    def unificar_partes(self, pasta_alvo, output_path=None):
        """
        Unifica arquivos divididos (_pt1, _pt2...) na mesma pasta.
        Retorna dict com: sucesso (bool), msg (str), unificados (int), erros (list).
        """
        if (
            not pasta_alvo
            or not os.path.exists(pasta_alvo)
            or not os.path.isdir(pasta_alvo)
        ):
            return {
                "sucesso": False,
                "msg": f"Diretório inválido ou inacessível: {pasta_alvo}",
                "unificados": 0,
                "erros": [],
            }

        # CORREÇÃO CRÍTICA: Apenas arquivos que CONTÊM _pt\d+ são agrupados.
        # Anteriormente, arquivos sem o padrão eram incluídos indevidamente no grupo.
        grupos = {}
        for f in os.listdir(pasta_alvo):
            if f.lower().endswith(".csv") and re.search(
                r"_pt\d+", f, flags=re.IGNORECASE
            ):
                base_name = re.sub(r"_pt\d+", "", f, flags=re.IGNORECASE)
                grupos.setdefault(base_name, []).append(os.path.join(pasta_alvo, f))

        if not grupos:
            return {
                "sucesso": True,
                "msg": "Nenhum arquivo com partes (_pt1, _pt2...) foi encontrado na pasta selecionada.",
                "unificados": 0,
                "erros": [],
            }

        unificados = 0
        erros = []

        for base, arquivos in grupos.items():
            # Ignora grupos com apenas 1 arquivo (não precisa unificar)
            if len(arquivos) < 2:
                print(f"Ignorado (apenas 1 parte): {base}")
                continue

            dfs = []
            for arq in sorted(arquivos):  # sort garante ordem correta: _pt1, _pt2...
                try:
                    dfs.append(self._tenta_ler_csv(arq))
                except Exception as e:
                    erros.append(f"Leitura de '{os.path.basename(arq)}': {e}")

            if not dfs:
                erros.append(f"Nenhuma parte legível para '{base}'")
                continue

            try:
                df_final = pd.concat(dfs, ignore_index=True)

                nome_final = f"UNIFICADO_{base}"
                if not nome_final.lower().endswith(".csv"):
                    nome_final += ".csv"

                caminho_saida = os.path.join(pasta_alvo, nome_final)
                df_final.to_csv(
                    caminho_saida, sep=";", index=False, encoding="utf-8-sig"
                )

                unificados += 1
                print(
                    f"Unificado: {nome_final} ({len(df_final):,} linhas de {len(dfs)} partes)"
                )

            except Exception as e:
                erros.append(f"Concat/escrita de '{base}': {e}")

        linhas_msg = f"{unificados} grupo(s) unificado(s) com sucesso."
        if erros:
            linhas_msg += f" | {len(erros)} erro(s) encontrado(s)."

        return {
            "sucesso": True,
            "msg": linhas_msg,
            "unificados": unificados,
            "erros": erros,
        }

    # --- LÓGICA DO RELATÓRIO DE COMPLETUDE ---
    def gerar_relatorio_completude(self, caminho_cmdb, caminho_master, caminho_saida):
        """Lógica avançada para cruzar a Base CMDB com as regras de completude do Master."""
        try:
            # 1. CARREGAMENTO E MAPEAMENTO DO MASTER
            try:
                df_master_raw = pd.read_excel(
                    caminho_master, sheet_name="Attributes", header=None
                )
                header_idx = -1
                for idx, row in df_master_raw.head(30).iterrows():
                    row_str = row.astype(str).str.lower().tolist()
                    if "variable" in row_str and (
                        "class" in row_str or "sys class name" in row_str
                    ):
                        header_idx = idx
                        break

                df_master = pd.read_excel(
                    caminho_master,
                    sheet_name="Attributes",
                    skiprows=header_idx if header_idx != -1 else 0,
                )
            except Exception as e:
                return False, f"Erro ao ler arquivo Master: {str(e)}"

            df_master.columns = df_master.columns.astype(str).str.strip().str.lower()

            mapa_clonagem = {
                "company": ["company", "u_company"],
                "business_criticality": [
                    "business_criticality",
                    "u_business_criticality",
                ],
                "sys_class_name": ["sys_class_name", "class", "sys class name"],
                "u_type_ref": ["u_type_ref", "type_ref"],
            }

            for destino, origens in mapa_clonagem.items():
                col_nome_filtro = f"{destino}.filtro"
                col_origem_master = next(
                    (c for c in df_master.columns if c in origens), None
                )
                if col_origem_master:
                    df_master[col_nome_filtro] = df_master[col_origem_master].copy()

            col_mandatoria = next(
                (c for c in df_master.columns if "mandatory" in c), None
            )
            col_atributo_master = (
                "variable" if "variable" in df_master.columns else None
            )
            candidatos_classe = [
                c
                for c in df_master.columns
                if c in ["class", "sys class name", "sys_class_name"]
            ]

            if not col_mandatoria or not col_atributo_master or not candidatos_classe:
                return False, f"Erro: Colunas cruciais não encontradas no Master."

            master_filtrado = df_master[
                df_master[col_mandatoria]
                .astype(str)
                .str.contains("Mandatory", case=False, na=False)
            ].copy()
            vars_mandatorias = set(
                master_filtrado[col_atributo_master].astype(str).str.strip().unique()
            )

            # 2. ANÁLISE DO CSV
            df_header = self._tenta_ler_csv(caminho_cmdb, nrows=0)
            cols_csv_orig = [c.strip() for c in df_header.columns.astype(str)]

            mapa_colunas_csv = {}
            col_classe_real = next(
                (c for c in cols_csv_orig if c.lower() in ["sys_class_name", "class"]),
                None,
            )
            if not col_classe_real:
                return False, "A coluna 'sys_class_name' não foi encontrada no CSV."
            mapa_colunas_csv["sys_class_name"] = col_classe_real

            col_company_real = next(
                (
                    c
                    for c in cols_csv_orig
                    if c.lower() in ["company", "u_company", "company_name"]
                ),
                None,
            )
            if col_company_real:
                mapa_colunas_csv["company"] = col_company_real

            col_bus_crit_real = next(
                (
                    c
                    for c in cols_csv_orig
                    if c.lower() in ["business_criticality", "u_business_criticality"]
                ),
                None,
            )
            if col_bus_crit_real:
                mapa_colunas_csv["business_criticality"] = col_bus_crit_real

            col_type_ref_real = next(
                (c for c in cols_csv_orig if c.lower() in ["u_type_ref", "type_ref"]),
                None,
            )
            if col_type_ref_real:
                mapa_colunas_csv["u_type_ref"] = col_type_ref_real

            # 3. DETECÇÃO DE MATCH
            df_amostra = self._tenta_ler_csv(caminho_cmdb, nrows=5000)
            classes_csv = set(
                df_amostra[col_classe_real]
                .dropna()
                .astype(str)
                .str.strip()
                .str.upper()
                .unique()
            )

            melhor_coluna_master = None
            maior_match = 0
            for col_cand in candidatos_classe:
                classes_master = set(
                    master_filtrado[col_cand]
                    .dropna()
                    .astype(str)
                    .str.strip()
                    .str.upper()
                    .unique()
                )
                match_count = len(classes_csv.intersection(classes_master))
                if match_count > maior_match:
                    maior_match = match_count
                    melhor_coluna_master = col_cand

            if not melhor_coluna_master:
                melhor_coluna_master = candidatos_classe[0]

            # 4. CARREGAMENTO E CLONAGEM NO CSV
            cols_base_csv = list(mapa_colunas_csv.values())
            cols_para_carregar = [
                c for c in cols_csv_orig if c in cols_base_csv or c in vars_mandatorias
            ]

            df_fonte = pd.read_csv(
                caminho_cmdb,
                sep=None,
                engine="python",
                encoding="utf-8-sig",
                usecols=cols_para_carregar,
                dtype=str,
                on_bad_lines="skip",
            )
            df_fonte.columns = df_fonte.columns.astype(str).str.strip()

            colunas_agrupamento_finais = []
            chaves_ordem = [
                "sys_class_name",
                "company",
                "business_criticality",
                "u_type_ref",
            ]

            for chave in chaves_ordem:
                nome_col_filtro = f"{chave}.filtro"
                if chave in mapa_colunas_csv:
                    nome_col_real = mapa_colunas_csv[chave]
                    if nome_col_real in df_fonte.columns:
                        df_fonte[nome_col_filtro] = df_fonte[nome_col_real].copy()
                        colunas_agrupamento_finais.append(nome_col_filtro)

            valores_nulos = [
                "",
                "No Data",
                "Without Data",
                "N/A",
                "null",
                "nan",
                "None",
            ]
            df_fonte.replace(valores_nulos, np.nan, inplace=True)
            gc.collect()

            grouped = df_fonte.groupby(colunas_agrupamento_finais, dropna=False)
            df_totais = grouped.size().reset_index(name="Total")
            df_preenchidos = grouped.count().reset_index()

            del df_fonte
            gc.collect()

            df_melted = df_preenchidos.melt(
                id_vars=colunas_agrupamento_finais,
                var_name="Atributo",
                value_name="Preenchidos",
            )
            df_calculo = pd.merge(
                df_melted, df_totais, on=colunas_agrupamento_finais, how="left"
            )

            # 5. O JOIN FINAL
            cols_master_select = [melhor_coluna_master, col_atributo_master]
            master_join = master_filtrado[cols_master_select].rename(
                columns={
                    melhor_coluna_master: "Classe_Alvo",
                    col_atributo_master: "Atributo_Alvo",
                }
            )

            master_join["Classe_Alvo"] = (
                master_join["Classe_Alvo"].astype(str).str.strip().str.upper()
            )
            master_join["Atributo_Alvo"] = (
                master_join["Atributo_Alvo"].astype(str).str.strip().str.upper()
            )

            col_join_classe = (
                "sys_class_name.filtro"
                if "sys_class_name.filtro" in df_calculo.columns
                else colunas_agrupamento_finais[0]
            )
            df_calculo["Classe_Join"] = (
                df_calculo[col_join_classe].astype(str).str.strip().str.upper()
            )
            df_calculo["Atributo_Join"] = (
                df_calculo["Atributo"].astype(str).str.strip().str.upper()
            )

            df_final = pd.merge(
                df_calculo,
                master_join,
                left_on=["Classe_Join", "Atributo_Join"],
                right_on=["Classe_Alvo", "Atributo_Alvo"],
                how="inner",
            )

            if df_final.empty:
                return (
                    False,
                    f"Relatório 0 linhas. Verifique compatibilidade de Classes.",
                )

            df_final["Total"] = df_final["Total"].fillna(0).astype(int)
            df_final["Preenchidos"] = df_final["Preenchidos"].fillna(0).astype(int)
            df_final["LinhasVazias"] = df_final["Total"] - df_final["Preenchidos"]
            df_final["PercentualPreenchido"] = np.where(
                df_final["Total"] > 0, df_final["Preenchidos"] / df_final["Total"], 0
            ).round(4)
            df_final["ColunaExiste"] = True

            colunas_remover = [
                "Classe_Join",
                "Atributo_Join",
                "Classe_Alvo",
                "Atributo_Alvo",
            ]
            cols_saida = [c for c in df_final.columns if c not in colunas_remover]
            df_final[cols_saida].to_excel(caminho_saida, index=False)

            return True, f"Sucesso! Relatório gerado em: {caminho_saida}"
        except Exception as e:
            return False, f"Erro Técnico:\n{traceback.format_exc()}"


class StructureValidator:
    """
    Validador de estrutura de dados que compara tabelas com modelo master.
    Versão Endurecida para Produção (Enterprise-Grade) com Visão de Raio-X.
    """

    def __init__(self):
        self.master_data = None
        self.master_path = None
        self._data_processor = DataProcessor()

    def carregar_master(self, caminho_master, force_reload=False):
        """Carrega o arquivo master de forma segura."""
        if (
            self.master_data is not None
            and self.master_path == caminho_master
            and not force_reload
        ):
            return self.master_data

        try:
            self.master_path = caminho_master

            if caminho_master.lower().endswith((".xlsx", ".xls")):
                try:
                    df = pd.read_excel(
                        caminho_master, sheet_name=None, engine="openpyxl"
                    )
                    print(f"Master carregado: {len(df)} aba(s) do Excel")
                    self.master_data = df
                    return df
                except Exception as e:
                    print(
                        f"Aviso: Erro ao ler Excel ({e}). Tentando fallback para CSV..."
                    )

            df = self._data_processor._tenta_ler_csv(caminho_master)
            self.master_data = {"Base": df}
            return self.master_data

        except Exception as e:
            self.master_data = None
            raise Exception(
                f"Falha crítica ao carregar arquivo master '{caminho_master}': {str(e)}"
            )

    def executar_validacao_completa(self, caminho_cmdb, caminho_master, caminho_saida):
        """
        Valida os campos mandatórios do Master contra as colunas da base CMDB.
        Gera um relatório Excel com CHECK = TRUE (presente) / FALSE (ausente) por campo/classe.

        Args:
            caminho_cmdb:   Caminho do arquivo CSV/Excel da base CMDB.
            caminho_master: Caminho do arquivo Excel com o modelo de dados (aba "Attributes").
            caminho_saida:  Caminho onde o Excel de resultado será salvo.

        Returns:
            (True, mensagem_sucesso) ou (False, mensagem_erro)
        """
        try:
            # ── 1. CARREGAR O MASTER ─────────────────────────────────────────────────
            xls = pd.ExcelFile(caminho_master, engine="openpyxl")
            aba_alvo = (
                "Attributes" if "Attributes" in xls.sheet_names else xls.sheet_names[0]
            )

            # Detecta linha do cabeçalho real (comum no CMDB: cabeçalho deslocado)
            df_raw = pd.read_excel(caminho_master, sheet_name=aba_alvo, header=None)
            header_idx = 0
            for idx in range(min(30, len(df_raw))):
                row_vals = df_raw.iloc[idx].astype(str).str.lower().tolist()
                tem_atributo = any(
                    k in row_vals for k in ["variable", "atributo", "field", "coluna"]
                )
                tem_mandatorio = any(
                    k in row_vals for k in ["mandatory", "obrigatorio", "mandatório"]
                )
                if tem_atributo and tem_mandatorio:
                    header_idx = idx
                    break

            df_master = pd.read_excel(
                caminho_master,
                sheet_name=aba_alvo,
                skiprows=header_idx,
                engine="openpyxl",
            )
            df_master.columns = df_master.columns.astype(str).str.strip().str.lower()

            # ── 2. IDENTIFICAR COLUNAS-CHAVE NO MASTER ──────────────────────────────
            col_mandatoria = next(
                (c for c in df_master.columns if "mandatory" in c or "obrigat" in c),
                None,
            )
            col_atributo = next(
                (
                    c
                    for c in df_master.columns
                    if c
                    in ["variable", "atributo", "field", "coluna", "name", "variável"]
                ),
                None,
            )
            col_classe = next(
                (
                    c
                    for c in df_master.columns
                    if c in ["class", "sys_class_name", "sys class name", "classe"]
                ),
                None,
            )
            col_descricao = next(
                (
                    c
                    for c in df_master.columns
                    if "descri" in c or "label" in c or "descript" in c
                ),
                None,
            )

            if not col_mandatoria:
                return False, (
                    "Coluna 'Mandatory' não encontrada no Master.\n"
                    f"Colunas disponíveis: {list(df_master.columns)}"
                )
            if not col_atributo:
                return False, (
                    "Coluna de atributos ('Variable' / 'Field') não encontrada no Master.\n"
                    f"Colunas disponíveis: {list(df_master.columns)}"
                )

            # ── 3. FILTRAR CAMPOS MANDATÓRIOS ───────────────────────────────────────
            mask_mandatorio = (
                df_master[col_mandatoria]
                .astype(str)
                .str.contains("Mandatory", case=False, na=False)
            )
            df_mandatorios = df_master[mask_mandatorio].copy()

            if df_mandatorios.empty:
                return False, (
                    "Nenhum campo marcado como 'Mandatory' foi encontrado no Master.\n"
                    f"Verifique a coluna '{col_mandatoria}'."
                )

            # ── 4. CARREGAR COLUNAS DO CMDB (somente cabeçalho — eficiente em RAM) ──
            try:
                df_cmdb_header = self._data_processor._tenta_ler_csv(
                    caminho_cmdb, nrows=0
                )
            except Exception as e:
                return False, f"Não foi possível ler o arquivo CMDB:\n{e}"

            cols_cmdb_upper = set(
                df_cmdb_header.columns.astype(str).str.strip().str.upper()
            )

            # ── 5. CRUZAMENTO: CADA CAMPO MANDATÓRIO vs CMDB ────────────────────────
            resultados = []
            campos_vistos = set()  # evita duplicatas

            for _, row in df_mandatorios.iterrows():
                atributo = str(row[col_atributo]).strip()
                if not atributo or atributo.upper() in ("NAN", "NONE", ""):
                    continue
                if atributo.upper() in campos_vistos:
                    continue
                campos_vistos.add(atributo.upper())

                classe = (
                    str(row[col_classe]).strip()
                    if col_classe and pd.notna(row.get(col_classe))
                    else "N/A"
                )
                mandatorio_val = str(row[col_mandatoria]).strip()
                descricao = (
                    str(row[col_descricao]).strip()
                    if col_descricao and pd.notna(row.get(col_descricao))
                    else ""
                )
                existe = atributo.upper() in cols_cmdb_upper

                resultados.append(
                    {
                        "Classe (sys_class_name)": classe,
                        "Campo Mandatório (Variable)": atributo,
                        "Tipo Mandatoriedade": mandatorio_val,
                        "Descrição": descricao,
                        "CHECK": existe,
                        "Status": (
                            "✅ Presente no CMDB" if existe else "❌ Ausente no CMDB"
                        ),
                        "Observação": (
                            ""
                            if existe
                            else "Campo mandatório não existe como coluna na base CMDB"
                        ),
                    }
                )

            if not resultados:
                return (
                    False,
                    "Nenhum resultado gerado. Verifique os arquivos selecionados.",
                )

            df_resultado = pd.DataFrame(resultados)

            # Ordenar: ausentes primeiro, depois por classe e campo
            df_resultado = df_resultado.sort_values(
                ["CHECK", "Classe (sys_class_name)", "Campo Mandatório (Variable)"]
            )

            # ── 6. SALVAR EXCEL COM FORMATAÇÃO ──────────────────────────────────────
            with pd.ExcelWriter(caminho_saida, engine="openpyxl") as writer:
                df_resultado.to_excel(writer, index=False, sheet_name="Validação CMDB")

                ws = writer.sheets["Validação CMDB"]

                # Ajuste automático de largura das colunas
                for col_cells in ws.columns:
                    max_len = max(
                        (len(str(cell.value)) if cell.value is not None else 0)
                        for cell in col_cells
                    )
                    ws.column_dimensions[col_cells[0].column_letter].width = min(
                        max_len + 4, 70
                    )

                # Colorir cabeçalho
                from openpyxl.styles import PatternFill, Font

                header_fill = PatternFill(
                    start_color="1F3864", end_color="1F3864", fill_type="solid"
                )
                header_font = Font(color="FFFFFF", bold=True)
                for cell in ws[1]:
                    cell.fill = header_fill
                    cell.font = header_font

                # Colorir linhas com CHECK = FALSE em vermelho claro
                red_fill = PatternFill(
                    start_color="FFCCCC", end_color="FFCCCC", fill_type="solid"
                )
                green_fill = PatternFill(
                    start_color="CCFFCC", end_color="CCFFCC", fill_type="solid"
                )
                col_check_idx = df_resultado.columns.get_loc("CHECK") + 1  # 1-indexed
                for row in ws.iter_rows(min_row=2, max_row=ws.max_row):
                    check_cell = row[col_check_idx - 1]
                    fill = green_fill if check_cell.value else red_fill
                    for cell in row:
                        cell.fill = fill

            presentes = int(df_resultado["CHECK"].sum())
            ausentes = len(df_resultado) - presentes

            return True, (
                f"✅ Validação concluída com sucesso!\n\n"
                f"Total de campos mandatórios verificados: {len(df_resultado)}\n"
                f"✅ Presentes na base CMDB:  {presentes}\n"
                f"❌ Ausentes no CMDB:        {ausentes}\n\n"
                f"Relatório salvo em:\n{caminho_saida}"
            )

        except Exception:
            return False, f"Erro técnico na validação:\n{traceback.format_exc()}"

    def validar_tabela(self, caminho_tabela, nome_aba_master=None, nome_tabela=None):
        """Valida a estrutura da tabela utilizando leituras tolerantes a falhas."""
        try:
            print(f"Validando tabela: {caminho_tabela}")

            if caminho_tabela.lower().endswith(".csv"):
                df_tabela = self._data_processor._tenta_ler_csv(caminho_tabela, nrows=5)
            else:
                df_tabela = pd.read_excel(caminho_tabela, nrows=5)

            if df_tabela is None or df_tabela.empty or len(df_tabela.columns) == 0:
                return False, "Tabela inválida ou vazia."

            colunas_tabela = set(df_tabela.columns.astype(str).str.strip().str.upper())

            if self.master_data is None:
                if not self.master_path:
                    self.master_path = self._encontrar_arquivo_master()
                    if not self.master_path:
                        return (
                            False,
                            "ARQUIVO MASTER INACESSÍVEL: Validação abortada para evitar falso-positivos.",
                        )
                self.carregar_master(self.master_path)

            modelo_correto = self._encontrar_modelo_no_master(
                nome_aba_master, nome_tabela, colunas_tabela
            )

            if not modelo_correto:
                return (
                    False,
                    "Nenhum modelo de dados compatível foi encontrado no arquivo Master.",
                )

            colunas_modelo = set(modelo_correto["colunas"])
            colunas_faltantes = colunas_modelo - colunas_tabela
            colunas_extra = colunas_tabela - colunas_modelo

            colunas_correspondentes = colunas_tabela.intersection(colunas_modelo)
            percentual = (
                round((len(colunas_correspondentes) / len(colunas_modelo) * 100), 2)
                if colunas_modelo
                else 0
            )

            resultado = {
                "tabela_analisada": caminho_tabela,
                "modelo_referencia": modelo_correto["fonte"],
                "total_colunas_modelo": len(colunas_modelo),
                "total_colunas_tabela": len(colunas_tabela),
                "colunas_faltantes": sorted(list(colunas_faltantes)),
                "colunas_extra": sorted(list(colunas_extra)),
                "colunas_correspondentes": len(colunas_correspondentes),
                "conformidade_perfeita": len(colunas_faltantes) == 0
                and len(colunas_extra) == 0,
                "percentual_conformidade": percentual,
                "aba_master_encontrada": modelo_correto["aba"],
                "similaridade": modelo_correto["similaridade"],
            }

            print(f"Validação concluída: {percentual:.1f}% de conformidade")
            return True, resultado

        except Exception as e:
            return False, f"Erro na validação de '{caminho_tabela}': {str(e)}"

    def _encontrar_arquivo_master(self):
        """Busca o arquivo master APENAS em locais homologados da rede."""
        caminho_base_rede = os.environ.get(
            "AUDIT_MASTER_PATH",
            r"\\bsbrsp56\AuditTM\TECNOLOGIA\AUDITORIAS\2026\Digitalizações\Digitalização - Gestão de Ativos\Digitalização (CMDB)\_BASE COMPLETA",
        )

        locais_oficiais = [
            os.path.join(caminho_base_rede, "Modelo de Dados - Master_v4.2.xlsx"),
            os.path.join(caminho_base_rede, "Modelo de Dados - Master_v4.2.csv"),
        ]

        for caminho in locais_oficiais:
            if os.path.exists(caminho):
                print(f"Master oficial encontrado: {caminho}")
                return caminho

        print("ALERTA: Arquivo Master oficial não localizado na rede.")
        return None

    def _encontrar_modelo_no_master(self, nome_aba, nome_tabela, colunas_tabela):
        melhor_match = None
        maior_similaridade = 0

        for aba_atual, df_master in self.master_data.items():
            if len(df_master.columns) < 1:
                continue

            candidatas = self._extrair_colunas_master_candidatas(df_master)
            for colunas_master in candidatas:
                similaridade = self._calcular_similaridade(
                    colunas_tabela, colunas_master
                )

                if similaridade > maior_similaridade:
                    maior_similaridade = similaridade
                    melhor_match = {
                        "fonte": f"Aba '{aba_atual}'",
                        "aba": aba_atual,
                        "colunas": colunas_master,
                        "similaridade": similaridade,
                    }

        if melhor_match and maior_similaridade > 0.05:
            return melhor_match
        return None

    def _extrair_colunas_master_candidatas(self, df_master):
        """RAIO-X: Procura o verdadeiro cabeçalho mesmo que não esteja na primeira linha."""
        candidatas = []
        dfs_para_analisar = [df_master]

        for i in range(min(20, len(df_master))):
            linha_str = df_master.iloc[i].astype(str).str.lower()
            if any(
                term in val
                for val in linha_str.values
                for term in [
                    "atributo",
                    "variable",
                    "coluna",
                    "name",
                    "class",
                    "parent",
                    "type",
                ]
            ):
                df_deslocado = df_master.iloc[i + 1 :].copy()
                df_deslocado.columns = df_master.iloc[i].values
                dfs_para_analisar.append(df_deslocado)

        for df in dfs_para_analisar:
            for col in df.columns:
                col_lower = str(col).lower()
                if any(
                    term in col_lower
                    for term in [
                        "atributo",
                        "variable",
                        "coluna",
                        "field",
                        "nome",
                        "name",
                        "variável",
                    ]
                ):
                    serie_limpa = df[col].dropna().astype(str).str.strip().str.upper()
                    colunas = set(serie_limpa[serie_limpa != ""])
                    if colunas:
                        candidatas.append(colunas)

            colunas_cabecalho = set(df.columns.astype(str).str.strip().str.upper())
            colunas_validas = set(
                c
                for c in colunas_cabecalho
                if c and not c.startswith("UNNAMED") and c != "NAN"
            )
            if colunas_validas:
                candidatas.append(colunas_validas)

        return candidatas

    def _calcular_similaridade(self, colunas_tabela, colunas_master):
        if not colunas_tabela or not colunas_master:
            return 0
        intersecao = colunas_tabela.intersection(colunas_master)
        return len(intersecao) / len(colunas_master)

    def definir_master_manual(self, caminho_master):
        self.master_path = caminho_master
        self.carregar_master(caminho_master, force_reload=True)


# Função de conveniência para uso rápido
def validar_estrutura_tabela(caminho_tabela, caminho_master=None, nome_aba=None):
    """
    Função rápida para validar uma tabela contra o master.
    Exemplo:
        sucesso, resultado = validar_estrutura_tabela("dados.csv")
    """
    validador = StructureValidator()
    if caminho_master:
        validador.definir_master_manual(caminho_master)
    return validador.validar_tabela(caminho_tabela, nome_aba)
