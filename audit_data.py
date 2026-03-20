import io
import os
import re
import gc
import traceback
from datetime import datetime
import pandas as pd
import numpy as np
from pathlib import Path


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS DE LEITURA ROBUSTA  (baseados em CMDB_Unificar_Bases.py)
# ─────────────────────────────────────────────────────────────────────────────


def _fix_unbalanced_quotes(raw_text: str) -> str:
    """Corrige quebras de linha com aspas desbalanceadas em CSVs mal-formados."""
    lines = raw_text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    fixed_lines, buffer = [], ""
    for line in lines:
        buffer += ("\n" + line) if buffer else line
        if buffer.count('"') % 2 == 0:
            fixed_lines.append(buffer)
            buffer = ""
    if buffer:
        fixed_lines.append(buffer)
    return "\n".join(fixed_lines)


def _read_csv_robust(path) -> pd.DataFrame:
    """
    Le CSV com multiplas heuristicas de fallback.
    Ordem: utf-8 auto-sep -> latin1 auto-sep -> fix-quotes fallback.
    """
    path = Path(path)
    for enc in ("utf-8", "latin1"):
        try:
            return pd.read_csv(
                path,
                engine="python",
                dtype=str,
                sep=None,
                encoding=enc,
                on_bad_lines="skip",
            )
        except Exception:
            pass
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            raw = f.read()
        fixed = _fix_unbalanced_quotes(raw)
        return pd.read_csv(io.StringIO(fixed), engine="python", dtype=str, sep=None)
    except Exception:
        pass
    raise RuntimeError(f"Falha ao ler CSV apos todos os fallbacks: {path}")


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS DO ALEX_VISAO  (logica de nulos por classe)
# ─────────────────────────────────────────────────────────────────────────────


def _contar_nulos_por_id(df: pd.DataFrame, id_col: str, colunas) -> pd.DataFrame:
    """
    Transforma colunas em linhas contando nulos e total por id_col.
    Replica def contar_nulos_por_id do Alex_visao_base_CMDB_V4.PY.
    """
    resultados = []
    for col in colunas:
        agrupado = (
            df.groupby(id_col)[col]
            .agg(nulos=lambda x: x.isna().sum(), total=lambda x: len(x))
            .reset_index()
        )
        agrupado["coluna"] = col
        resultados.append(agrupado[[id_col, "coluna", "nulos", "total"]])
    return pd.concat(resultados, ignore_index=True)


def _contar_computer(
    df: pd.DataFrame, id_col: str, colunas, col_ref: str = "sys_class_name"
) -> pd.DataFrame:
    """
    Variante para a class 'Computer': agrupa por u_category dentro
    do subconjunto onde col_ref == 'Computer'.
    Replica def contar_computer do Alex_visao_base_CMDB_V4.PY.
    """
    resultados = []
    df_filtrado = df[df[col_ref] == "Computer"]
    for col in colunas:
        agrupado = (
            df_filtrado.groupby(id_col)[col]
            .agg(nulos=lambda x: x.isna().sum(), total=lambda x: len(x))
            .reset_index()
        )
        agrupado["coluna"] = col
        resultados.append(agrupado[[id_col, "coluna", "nulos", "total"]])
    return pd.concat(resultados, ignore_index=True)


# ─────────────────────────────────────────────────────────────────────────────
# HELPER: LE O MASTER CORRETAMENTE
# ─────────────────────────────────────────────────────────────────────────────


def _carregar_master_attributes(caminho_master: str) -> pd.DataFrame:
    """
    Le a aba 'Attributes' do Master, seleciona colunas 4:26,
    filtra Mandatory == 'Mandatory'.

    Estrutura validada no Modelo_de_Dados_-_Master_v4.2.xlsx:
      Colunas 4:26 = Module, Class, Sys Class Name, Level, Path, Attribute,
                     Variable, Type, Reference, Max length, Default value,
                     Definition, Section, Mandatory, Discovery, Automation,
                     Integration, Order
    Nota: NAO usa skiprows (o codigo de referencia usava uma versao antiga do Excel).
    """
    df = pd.read_excel(caminho_master, sheet_name="Attributes", engine="openpyxl")
    df = df.iloc[:, 4:26]
    df = df[df["Mandatory"] == "Mandatory"].copy()
    return df


# ─────────────────────────────────────────────────────────────────────────────
# DATA PROCESSOR
# ─────────────────────────────────────────────────────────────────────────────


class DataProcessor:

    def __init__(self):
        pass

    # ---------- utilitarios --------------------------------------------------

    def contar_linhas(self, path):
        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                count = sum(1 for _ in f)
            return max(0, count - 1)
        except Exception:
            return 0

    def _tenta_ler_csv(self, filepath, nrows=None):
        tentativas = [
            {"enc": "utf-8", "sep": ";"},
            {"enc": "latin-1", "sep": ";"},
            {"enc": "utf-8-sig", "sep": ";"},
            {"enc": "utf-8", "sep": ","},
            {"enc": "latin-1", "sep": ","},
        ]
        last_exc = None
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
                last_exc = e
        if last_exc:
            raise last_exc
        raise ValueError("Formato CSV nao reconhecido.")

    # ---------- historico ----------------------------------------------------

    def listar_historico(self, root_path, dias_filtro):
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
                            print(f"Aviso: '{file}': {e}")
        except Exception as e:
            print(f"Erro scan historico: {e}")
        dados.sort(
            key=lambda x: (
                x["pasta_pai"],
                datetime.strptime(x["data"], "%d/%m/%Y %H:%M"),
            ),
            reverse=True,
        )
        return dados, len(dados)

    def verificar_pendencias(self, root_path, mes_ano_alvo, lista_esperada_nomes):
        arquivos_na_pasta = set()
        pastas_encontradas = []
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
                "Nenhuma pasta com este mes encontrada na rede.",
            )
        faltantes = [
            n for n in lista_esperada_nomes if n.lower() not in arquivos_na_pasta
        ]
        return faltantes, caminho_display

    def verificar_integridade(self, path):
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

    # ---------- gerar master (CMDB_Unificar_Bases.py) ------------------------

    def gerar_master(self, root_path, mes_alvo, output_path=None):
        """
        Unifica todos os CSVs/Excel do mes em um unico arquivo MASTER.

        Logica primaria (CMDB_Unificar_Bases.py):
          ROOT_DIR / ClasseDir / MesDir / arquivos
          Adiciona coluna 'Module' = nome da ClasseDir.
          Saida padrao: ROOT_DIR / '1. Base Completa' / CMDB_{mes_alvo}.csv

        Fallback: varredura recursiva para qualquer pasta cujo nome == mes_alvo.
        """
        print(f"\nGerador Master | Mes: {mes_alvo} | Raiz: {root_path}")

        root = Path(root_path)
        rows = []
        arquivos_ok = 0
        arquivos_erro = []

        # abordagem primaria: ROOT/Modulo/Mes/arquivos
        for classe_dir in sorted(root.iterdir()):
            if not classe_dir.is_dir():
                continue
            month_folder = classe_dir / mes_alvo
            if not month_folder.exists():
                continue
            modulo = classe_dir.name
            print(f"  Modulo: {modulo}")
            for file in sorted(month_folder.glob("*.*")):
                if file.suffix.lower() not in (".csv", ".xlsx", ".xls"):
                    continue
                if "master" in file.name.lower() or file.name.startswith("~$"):
                    continue
                try:
                    if file.suffix.lower() == ".csv":
                        df = _read_csv_robust(file)
                    else:
                        df = pd.read_excel(
                            file, sheet_name=0, dtype=str, engine="openpyxl"
                        )
                    df["Module"] = modulo
                    rows.append(df)
                    arquivos_ok += 1
                    print(f"    OK  {file.name}  ({len(df):,} linhas)")
                except Exception as e:
                    arquivos_erro.append(f"{file.name}: {e}")
                    print(f"    ERRO  {file.name}: {e}")

        # fallback: varredura recursiva
        if not rows:
            print("  Abordagem primaria sem resultados — varredura recursiva...")
            for r, _dirs, files in os.walk(root_path):
                if os.path.basename(r) == mes_alvo:
                    modulo = os.path.basename(os.path.dirname(r))
                    print(f"  Pasta encontrada: {r}")
                    for fname in sorted(files):
                        ext = os.path.splitext(fname)[1].lower()
                        if ext not in (".csv", ".xlsx", ".xls"):
                            continue
                        if "master" in fname.lower() or fname.startswith("~$"):
                            continue
                        full = Path(r) / fname
                        try:
                            df = (
                                _read_csv_robust(full)
                                if ext == ".csv"
                                else pd.read_excel(full, sheet_name=0, dtype=str)
                            )
                            df["Module"] = modulo
                            rows.append(df)
                            arquivos_ok += 1
                            print(f"    OK  {fname}  ({len(df):,} linhas)")
                        except Exception as e:
                            arquivos_erro.append(f"{fname}: {e}")

        if not rows:
            print(f"  Nenhum arquivo encontrado para o mes {mes_alvo}.")
            return None

        try:
            df_final = pd.concat(rows, ignore_index=True, sort=False)
            cols_ordered = ["Module"] + [c for c in df_final.columns if c != "Module"]
            df_final = df_final.reindex(columns=cols_ordered)

            if not output_path:
                nome_arq = f"CMDB_{mes_alvo}.csv"
                output_dir = root / "1. Base Completa"
                output_dir.mkdir(parents=True, exist_ok=True)
                output_path = str(output_dir / nome_arq)

            if os.path.exists(output_path):
                os.remove(output_path)

            df_final.to_csv(output_path, index=False, encoding="utf-8-sig")
            print(f"\n  Master gerado: {output_path}")
            print(f"  Arquivos processados: {arquivos_ok}")
            print(f"  Total de linhas:      {len(df_final):,}")
            if arquivos_erro:
                print(f"  Erros ignorados:      {len(arquivos_erro)}")
            return output_path

        except Exception as e:
            print(f"Erro critico ao consolidar: {e}")
            return None

    # ---------- unificar partes ----------------------------------------------

    def unificar_partes(self, pasta_alvo, output_path=None):
        if (
            not pasta_alvo
            or not os.path.exists(pasta_alvo)
            or not os.path.isdir(pasta_alvo)
        ):
            return {
                "sucesso": False,
                "msg": f"Diretorio invalido: {pasta_alvo}",
                "unificados": 0,
                "erros": [],
            }

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
                "msg": "Nenhum arquivo com partes (_pt1, _pt2...) encontrado.",
                "unificados": 0,
                "erros": [],
            }

        unificados = 0
        erros = []
        for base, arquivos in grupos.items():
            if len(arquivos) < 2:
                continue
            dfs = []
            for arq in sorted(arquivos):
                try:
                    dfs.append(self._tenta_ler_csv(arq))
                except Exception as e:
                    erros.append(f"Leitura de '{os.path.basename(arq)}': {e}")
            if not dfs:
                erros.append(f"Nenhuma parte legivel para '{base}'")
                continue
            try:
                df_final = pd.concat(dfs, ignore_index=True)
                nome_final = f"UNIFICADO_{base}"
                if not nome_final.lower().endswith(".csv"):
                    nome_final += ".csv"
                df_final.to_csv(
                    os.path.join(pasta_alvo, nome_final),
                    sep=";",
                    index=False,
                    encoding="utf-8-sig",
                )
                unificados += 1
            except Exception as e:
                erros.append(f"Concat/escrita de '{base}': {e}")

        msg = f"{unificados} grupo(s) unificado(s) com sucesso."
        if erros:
            msg += f" | {len(erros)} erro(s)."
        return {"sucesso": True, "msg": msg, "unificados": unificados, "erros": erros}

    # ---------- relatorio de completude (Alex_visao_base_CMDB_V4.PY) ---------

    def gerar_relatorio_completude(
        self, caminho_cmdb: str, caminho_master: str, caminho_saida: str
    ):
        """
        Cruza a base CMDB com os campos mandatorios do Master.
        Exporta Excel com 3 abas:
          1. visao_datamaster_brasil  — TODOS os campos mandatórios do Master,
                                       incluindo os sem correspondência no CMDB
                                       (nulos/ausentes aparecem com células vazias)
          2. completude_brasil        — resumo de cobertura por classe
          3. atributo_sem_mandatorio  — classes no CMDB sem referencia no Master

        Logica fiel ao Alex_visao_base_CMDB_V4.PY.
        Leitura correta do Master: aba 'Attributes', iloc[:,4:26], Mandatory=='Mandatory'.

        ALTERAÇÃO: df_master_filtrado agora inclui registros left_only (campos
        mandatórios do Master que NÃO encontraram correspondência no CMDB),
        exibindo-os com nulos/percentual_nulos em branco para identificação.
        """
        try:
            # 1. Master
            print("\n-- Carregando Master --")
            try:
                data_master = _carregar_master_attributes(caminho_master)
            except Exception as e:
                return False, f"Erro ao ler arquivo Master:\n{e}"
            print(f"   Campos mandatorios: {len(data_master)}")
            print(f"   Classes unicas:     {data_master['Class'].nunique()}")

            # 2. CMDB
            print("\n-- Carregando CMDB --")
            try:
                dados = _read_csv_robust(Path(caminho_cmdb))
            except Exception as e:
                return False, f"Erro ao ler base CMDB:\n{e}"

            if "sys_class_name" not in dados.columns:
                return False, (
                    "Coluna 'sys_class_name' nao encontrada na base CMDB.\n"
                    f"Colunas disponiveis: {list(dados.columns[:15])}"
                )
            print(f"   Linhas CMDB:           {len(dados):,}")
            print(f"   Classes unicas (CMDB): {dados['sys_class_name'].nunique()}")

            # 3. Contagem nulos — classes != 'Computer'
            print("\n-- Processando classes nao-Computer --")
            colunas_para_contar = dados.columns.drop("sys_class_name", errors="ignore")
            resultado_nao_computer = _contar_nulos_por_id(
                dados, "sys_class_name", colunas_para_contar
            )
            resultado_nao_computer = resultado_nao_computer[
                resultado_nao_computer["sys_class_name"] != "Computer"
            ]
            print(f"   Combinacoes geradas: {len(resultado_nao_computer):,}")

            # 4. Contagem nulos — class 'Computer' via u_category
            print("\n-- Processando classe Computer --")
            df_resultado_computer = pd.DataFrame()
            tem_computer = (dados["sys_class_name"] == "Computer").any()
            tem_u_category = "u_category" in dados.columns

            if tem_computer and tem_u_category:
                colunas_computer = dados.columns.drop("u_category", errors="ignore")
                df_resultado_computer = _contar_computer(
                    dados, "u_category", colunas_computer, col_ref="sys_class_name"
                )
                df_resultado_computer = df_resultado_computer.rename(
                    columns={"u_category": "sys_class_name"}
                )
                mapa_computer = {
                    "ATM": "Computer (ATM / Finance Device)",
                    "Financial Device": "Computer (ATM / Finance Device)",
                    "Personal Computer": "Computer (Personal Computer / Virtual Desktop)",
                }
                for val_csv, val_master in mapa_computer.items():
                    df_resultado_computer.loc[
                        df_resultado_computer["sys_class_name"] == val_csv,
                        "sys_class_name",
                    ] = val_master
                print(f"   Combinacoes geradas: {len(df_resultado_computer):,}")
            else:
                motivo = []
                if not tem_computer:
                    motivo.append("classe 'Computer' nao encontrada no CMDB")
                if not tem_u_category:
                    motivo.append("coluna 'u_category' ausente")
                print(f"   Ignorado ({'; '.join(motivo)}).")

            # 5. Unifica
            partes = [resultado_nao_computer]
            if not df_resultado_computer.empty:
                partes.append(df_resultado_computer)
            resultados_mesclados = pd.concat(partes, ignore_index=True, axis=0)

            # 6. Normaliza para UPPER
            resultados_mesclados[["sys_class_name", "coluna"]] = resultados_mesclados[
                ["sys_class_name", "coluna"]
            ].apply(lambda x: x.astype(str).str.upper())
            data_master = data_master.copy()
            data_master[["Class", "Variable"]] = data_master[
                ["Class", "Variable"]
            ].apply(lambda x: x.astype(str).str.upper())

            # 7. Percentual de nulos
            resultados_mesclados["percentual_nulos"] = (
                resultados_mesclados["nulos"] / resultados_mesclados["total"] * 100
            ).round(2)

            print(
                f"\n   Total combinacoes classe*campo CMDB: {len(resultados_mesclados):,}"
            )

            # 8. Merge Master (left) <- CMDB
            print("\n-- Cruzando Master com CMDB --")
            df_master_merged = data_master.merge(
                resultados_mesclados,
                left_on=["Class", "Variable"],
                right_on=["sys_class_name", "coluna"],
                how="left",
                indicator=True,
            )

            # ─────────────────────────────────────────────────────────────────
            # ALTERAÇÃO: inclui TODOS os registros do Master no relatório,
            # inclusive os "left_only" (campos mandatórios sem dados no CMDB).
            # Antes: filtrava fora os left_only — agora eles aparecem com
            # nulos/total/percentual_nulos em branco, permitindo identificar
            # campos obrigatórios completamente ausentes na base CMDB.
            # ─────────────────────────────────────────────────────────────────
            df_master_filtrado = df_master_merged.copy()

            colunas_saida = [
                c
                for c in [
                    "Class",
                    "Sys Class Name",
                    "Level",
                    "Path",
                    "Variable",
                    "Type",
                    "Reference",
                    "Max length",
                    "Default value",
                    "Definition",
                    "Section",
                    "Mandatory",
                    "Discovery",
                    "Automation",
                    "Integration",
                    "Order",
                    "Module",
                    "sys_class_name",
                    "coluna",
                    "nulos",
                    "total",
                    "percentual_nulos",
                    "_merge",
                ]
                if c in df_master_filtrado.columns
            ]
            df_master_filtrado = df_master_filtrado[colunas_saida]
            print(f"   Registros na visao (com nulos): {len(df_master_filtrado):,}")

            # 9. Analise de completude por classe
            analise_completude_global = (
                df_master_merged.groupby(["Class", "_merge"])
                .size()
                .unstack(fill_value=0)
                .reset_index()
            ).sort_values(by="left_only", ascending=False)
            analise_completude_global["Total"] = (
                analise_completude_global.select_dtypes(include="number").sum(axis=1)
            )
            if "both" in analise_completude_global.columns:
                analise_completude_global["percentual"] = (
                    analise_completude_global["both"]
                    / analise_completude_global["Total"]
                    * 100
                ).round(2)
                analise_completude_brasil = analise_completude_global[
                    analise_completude_global["both"] != 0
                ].copy()
            else:
                analise_completude_brasil = analise_completude_global.copy()

            # 10. Classes no CMDB sem referencia no Master
            df_master_comp = pd.DataFrame(
                data_master["Class"].dropna().unique(), columns=[0]
            )
            df_dados_comp = pd.DataFrame(
                resultados_mesclados["sys_class_name"].dropna().unique(), columns=[0]
            )
            df_sem_mandatorio = df_master_comp.merge(
                df_dados_comp, on=0, how="outer", indicator=True
            )
            df_sem_mandatorio = df_sem_mandatorio[
                df_sem_mandatorio["_merge"] == "right_only"
            ].rename(columns={0: "sys_class_name_somente_cmdb"})

            # 11. Exporta Excel 3 abas
            print(f"\n-- Exportando Excel: {caminho_saida} --")
            with pd.ExcelWriter(caminho_saida, engine="openpyxl") as writer:
                df_master_filtrado.to_excel(
                    writer, sheet_name="visao_datamaster_brasil", index=False
                )
                analise_completude_brasil.to_excel(
                    writer, sheet_name="completude_brasil", index=False
                )
                df_sem_mandatorio.to_excel(
                    writer, sheet_name="atributo_sem_mandatorio", index=False
                )

            # Métricas do resumo — conta apenas os que têm dados no CMDB
            df_com_dados = df_master_filtrado[
                df_master_filtrado["_merge"] != "left_only"
            ]
            media_nulos = (
                df_com_dados["percentual_nulos"].mean()
                if "percentual_nulos" in df_com_dados.columns
                else 0
            )
            classes_brasil = (
                analise_completude_brasil["Class"].nunique()
                if "Class" in analise_completude_brasil.columns
                else 0
            )
            sem_dados_cmdb = int((df_master_filtrado["_merge"] == "left_only").sum())
            return True, (
                f"Relatorio gerado com sucesso!\n\n"
                f"Total de campos mandatórios:      {len(df_master_filtrado):,}\n"
                f"  ✅ Com dados no CMDB:           {len(df_com_dados):,}\n"
                f"  ❌ Ausentes no CMDB (nulos):    {sem_dados_cmdb:,}\n"
                f"Classes com mandatorios:           {classes_brasil}\n"
                f"Media de % nulos (campos c/ dados): {media_nulos:.1f}%\n\n"
                f"Arquivo salvo em:\n{caminho_saida}"
            )

        except Exception:
            return False, f"Erro tecnico:\n{traceback.format_exc()}"


# ─────────────────────────────────────────────────────────────────────────────
# STRUCTURE VALIDATOR
# ─────────────────────────────────────────────────────────────────────────────


class StructureValidator:
    """
    Valida se os campos mandatorios do Master estao presentes como
    colunas na base CMDB. Gera relatorio Excel com CHECK por campo/classe.
    """

    def __init__(self):
        self.master_data = None
        self.master_path = None
        self._data_processor = DataProcessor()

    def carregar_master(self, caminho_master, force_reload=False):
        if (
            self.master_data is not None
            and self.master_path == caminho_master
            and not force_reload
        ):
            return self.master_data
        try:
            self.master_path = caminho_master
            if caminho_master.lower().endswith((".xlsx", ".xls")):
                df = pd.read_excel(caminho_master, sheet_name=None, engine="openpyxl")
                self.master_data = df
                return df
            df = self._data_processor._tenta_ler_csv(caminho_master)
            self.master_data = {"Base": df}
            return self.master_data
        except Exception as e:
            self.master_data = None
            raise Exception(f"Falha ao carregar master '{caminho_master}': {e}")

    def executar_validacao_completa(
        self, caminho_cmdb: str, caminho_master: str, caminho_saida: str
    ):
        """
        Valida campos mandatorios do Master vs colunas da base CMDB.
        Gera Excel com CHECK Presente/Ausente por campo e classe.

        Leitura correta do Master:
          - Aba 'Attributes', colunas 4:26 (sem skiprows)
          - Filtro Mandatory == 'Mandatory'
          - Chaves: 'Class' (= sys_class_name) e 'Variable' (= coluna CMDB)
        """
        try:
            # 1. Master
            print("\n-- Carregando Master (executar_validacao_completa) --")
            try:
                df_mandatorios = _carregar_master_attributes(caminho_master)
            except Exception as e:
                return False, (
                    f"Erro ao ler o arquivo Master:\n{e}\n\n"
                    "Verifique se o arquivo possui a aba 'Attributes'."
                )

            if df_mandatorios.empty:
                return False, (
                    "Nenhum campo 'Mandatory' encontrado no Master.\n"
                    "Verifique a coluna 'Mandatory'."
                )
            print(f"   Campos mandatorios: {len(df_mandatorios)}")
            print(f"   Classes:            {df_mandatorios['Class'].nunique()}")

            # 2. Cabecalho do CMDB (eficiente em RAM)
            print("\n-- Lendo cabecalho do CMDB --")
            try:
                df_header = self._data_processor._tenta_ler_csv(caminho_cmdb, nrows=0)
            except Exception as e:
                return False, f"Nao foi possivel ler o arquivo CMDB:\n{e}"

            cols_cmdb_upper = set(df_header.columns.astype(str).str.strip().str.upper())
            print(f"   Colunas no CMDB: {len(cols_cmdb_upper)}")

            # 3. Cruzamento campo a campo
            print("\n-- Cruzando campos mandatorios vs colunas CMDB --")
            resultados = []
            campos_vistos = set()

            col_classe = "Class"
            col_atributo = "Variable"
            col_descricao = (
                "Definition" if "Definition" in df_mandatorios.columns else None
            )
            col_mandatory = "Mandatory"

            for _, row in df_mandatorios.iterrows():
                atributo = str(row[col_atributo]).strip()
                if not atributo or atributo.upper() in ("NAN", "NONE", ""):
                    continue
                chave = (str(row[col_classe]).strip().upper(), atributo.upper())
                if chave in campos_vistos:
                    continue
                campos_vistos.add(chave)

                classe = (
                    str(row[col_classe]).strip()
                    if pd.notna(row.get(col_classe))
                    else "N/A"
                )
                descricao = (
                    str(row[col_descricao]).strip()
                    if col_descricao and pd.notna(row.get(col_descricao))
                    else ""
                )
                existe = atributo.upper() in cols_cmdb_upper
                resultados.append(
                    {
                        "Classe (sys_class_name)": classe,
                        "Campo Mandatorio (Variable)": atributo,
                        "Tipo Mandatoriedade": str(row[col_mandatory]).strip(),
                        "Definicao": descricao,
                        "CHECK": existe,
                        "Status": "Presente no CMDB" if existe else "Ausente no CMDB",
                        "Observacao": (
                            ""
                            if existe
                            else "Campo mandatorio nao existe como coluna na base CMDB"
                        ),
                    }
                )

            if not resultados:
                return False, "Nenhum resultado gerado. Verifique os arquivos."

            df_resultado = pd.DataFrame(resultados)
            df_resultado = df_resultado.sort_values(
                ["CHECK", "Classe (sys_class_name)", "Campo Mandatorio (Variable)"]
            )
            presentes = int(df_resultado["CHECK"].sum())
            ausentes = len(df_resultado) - presentes
            print(f"   Presentes: {presentes}  |  Ausentes: {ausentes}")

            # 4. Salva Excel formatado
            print(f"\n-- Salvando em: {caminho_saida} --")
            with pd.ExcelWriter(caminho_saida, engine="openpyxl") as writer:
                df_resultado.to_excel(writer, index=False, sheet_name="Validacao CMDB")
                ws = writer.sheets["Validacao CMDB"]

                for col_cells in ws.columns:
                    max_len = max(
                        (len(str(c.value)) if c.value is not None else 0)
                        for c in col_cells
                    )
                    ws.column_dimensions[col_cells[0].column_letter].width = min(
                        max_len + 4, 70
                    )

                from openpyxl.styles import PatternFill, Font

                hdr_fill = PatternFill(
                    start_color="1F3864", end_color="1F3864", fill_type="solid"
                )
                hdr_font = Font(color="FFFFFF", bold=True)
                for cell in ws[1]:
                    cell.fill = hdr_fill
                    cell.font = hdr_font

                red_fill = PatternFill(
                    start_color="FFCCCC", end_color="FFCCCC", fill_type="solid"
                )
                green_fill = PatternFill(
                    start_color="CCFFCC", end_color="CCFFCC", fill_type="solid"
                )
                check_col_idx = df_resultado.columns.get_loc("CHECK")
                for row in ws.iter_rows(min_row=2, max_row=ws.max_row):
                    fill = green_fill if row[check_col_idx].value else red_fill
                    for cell in row:
                        cell.fill = fill

            return True, (
                f"Validacao concluida com sucesso!\n\n"
                f"Total de campos mandatorios verificados: {len(df_resultado)}\n"
                f"Presentes na base CMDB:  {presentes}\n"
                f"Ausentes no CMDB:        {ausentes}\n\n"
                f"Relatorio salvo em:\n{caminho_saida}"
            )

        except Exception:
            return False, f"Erro tecnico na validacao:\n{traceback.format_exc()}"

    def validar_tabela(self, caminho_tabela, nome_aba_master=None, nome_tabela=None):
        try:
            if caminho_tabela.lower().endswith(".csv"):
                df_tabela = self._data_processor._tenta_ler_csv(caminho_tabela, nrows=5)
            else:
                df_tabela = pd.read_excel(caminho_tabela, nrows=5)
            if df_tabela is None or df_tabela.empty or len(df_tabela.columns) == 0:
                return False, "Tabela invalida ou vazia."
            colunas_tabela = set(df_tabela.columns.astype(str).str.strip().str.upper())
            if self.master_data is None:
                if not self.master_path:
                    self.master_path = self._encontrar_arquivo_master()
                    if not self.master_path:
                        return False, "ARQUIVO MASTER INACESSIVEL."
                self.carregar_master(self.master_path)
            modelo = self._encontrar_modelo_no_master(
                nome_aba_master, nome_tabela, colunas_tabela
            )
            if not modelo:
                return False, "Nenhum modelo compativel encontrado no Master."
            colunas_modelo = set(modelo["colunas"])
            faltantes = colunas_modelo - colunas_tabela
            extras = colunas_tabela - colunas_modelo
            correspondentes = colunas_tabela.intersection(colunas_modelo)
            percentual = (
                round(len(correspondentes) / len(colunas_modelo) * 100, 2)
                if colunas_modelo
                else 0
            )
            return True, {
                "tabela_analisada": caminho_tabela,
                "modelo_referencia": modelo["fonte"],
                "total_colunas_modelo": len(colunas_modelo),
                "total_colunas_tabela": len(colunas_tabela),
                "colunas_faltantes": sorted(faltantes),
                "colunas_extra": sorted(extras),
                "colunas_correspondentes": len(correspondentes),
                "conformidade_perfeita": not faltantes and not extras,
                "percentual_conformidade": percentual,
                "aba_master_encontrada": modelo["aba"],
                "similaridade": modelo["similaridade"],
            }
        except Exception as e:
            return False, f"Erro na validacao de '{caminho_tabela}': {e}"

    def _encontrar_arquivo_master(self):
        caminho_base = os.environ.get("AUDIT_MASTER_PATH", "")
        for nome in (
            "Modelo de Dados - Master_v4.2.xlsx",
            "Modelo de Dados - Master_v4.2.csv",
        ):
            c = os.path.join(caminho_base, nome) if caminho_base else ""
            if c and os.path.exists(c):
                return c
        return None

    def _encontrar_modelo_no_master(self, nome_aba, nome_tabela, colunas_tabela):
        melhor = None
        maior_sim = 0
        for aba, df in self.master_data.items():
            if len(df.columns) < 1:
                continue
            for colunas_master in self._extrair_colunas_master_candidatas(df):
                sim = self._calcular_similaridade(colunas_tabela, colunas_master)
                if sim > maior_sim:
                    maior_sim = sim
                    melhor = {
                        "fonte": f"Aba '{aba}'",
                        "aba": aba,
                        "colunas": colunas_master,
                        "similaridade": sim,
                    }
        return melhor if melhor and maior_sim > 0.05 else None

    def _extrair_colunas_master_candidatas(self, df_master):
        candidatas = []
        dfs = [df_master]
        for i in range(min(20, len(df_master))):
            vals = df_master.iloc[i].astype(str).str.lower()
            if any(
                t in v
                for v in vals.values
                for t in ["atributo", "variable", "coluna", "name", "class", "type"]
            ):
                df_des = df_master.iloc[i + 1 :].copy()
                df_des.columns = df_master.iloc[i].values
                dfs.append(df_des)
        for df in dfs:
            for col in df.columns:
                if any(
                    t in str(col).lower()
                    for t in ["atributo", "variable", "coluna", "field", "nome", "name"]
                ):
                    serie = df[col].dropna().astype(str).str.strip().str.upper()
                    colunas = set(serie[serie != ""])
                    if colunas:
                        candidatas.append(colunas)
            cab = set(df.columns.astype(str).str.strip().str.upper())
            validas = {
                c for c in cab if c and not c.startswith("UNNAMED") and c != "NAN"
            }
            if validas:
                candidatas.append(validas)
        return candidatas

    def _calcular_similaridade(self, colunas_tabela, colunas_master):
        if not colunas_tabela or not colunas_master:
            return 0
        return len(colunas_tabela.intersection(colunas_master)) / len(colunas_master)

    def definir_master_manual(self, caminho_master):
        self.master_path = caminho_master
        self.carregar_master(caminho_master, force_reload=True)


def validar_estrutura_tabela(caminho_tabela, caminho_master=None, nome_aba=None):
    """Funcao de conveniencia para validacao rapida."""
    v = StructureValidator()
    if caminho_master:
        v.definir_master_manual(caminho_master)
    return v.validar_tabela(caminho_tabela, nome_aba)
