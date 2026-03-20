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
                # Data de corte para performance
                from datetime import timedelta

                limite_data = datetime.now() - timedelta(days=dias_filtro)

            # Varredura recursiva (os.walk)
            for root, dirs, files in os.walk(root_path):
                pasta_atual = os.path.basename(root)

                # --- LÓGICA DE AGRUPAMENTO ---
                # Verifica se a pasta atual é um mês (formato MM.YYYY)
                is_mes = re.match(r"^\d{2}\.\d{4}$", pasta_atual)

                for file in files:
                    if file.lower().endswith(".csv") and "master" not in file.lower():
                        full_path = os.path.join(root, file)
                        try:
                            stats = os.stat(full_path)
                            dt_mod = datetime.fromtimestamp(stats.st_mtime)

                            # Filtro de dias
                            if limite_data and dt_mod < limite_data:
                                continue

                            tag = "old" if (datetime.now() - dt_mod).days > 7 else "new"

                            # Nome do Relatório (Pasta acima do mês)
                            nome_relatorio = os.path.basename(os.path.dirname(root))

                            # DEFINIÇÃO DO GRUPO PARA A UI:
                            if is_mes:
                                # Se estiver dentro de uma pasta de mês, agrupa pelo MÊS (ex: "01.2024")
                                grupo_visual = pasta_atual
                                nome_visual = file
                            else:
                                # Se não estiver em pasta de mês, usa o caminho relativo para organizar
                                grupo_visual = f"Outros > {pasta_atual}"
                                nome_visual = file

                            # Contagem de linhas (opcional, removível se causar lentidão em rede lenta)
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

        # Ordena: Primeiro por Grupo (Mês), depois por Data do arquivo (mais recente primeiro)
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

        # 1. Busca Profunda
        for root, dirs, files in os.walk(root_path):
            nome_pasta = os.path.basename(root)

            # Se a pasta atual é o mês alvo
            if nome_pasta == mes_ano_alvo:
                pastas_encontradas.append(root)
                for f in files:
                    if f.lower().endswith(".csv"):
                        nome_sem_ext = os.path.splitext(f)[0]
                        # Remove sufixos de cópia
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
        # 2. Verifica o que falta
        faltantes = []
        for nome in lista_esperada_nomes:
            # Compara lowercase
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
                        # Tenta ler apenas 5 linhas com engines flexíveis
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
            {"enc": "utf-8-sig", "sep": ";"},  # Para Excel com BOM
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
                    engine="python",  # Mais lento mas mais permissivo
                )
                if df.shape[1] > 1:  # Sucesso: conseguiu separar colunas
                    return df
            except Exception as e:
                last_exception = e
                continue

        # Se chegou aqui, nenhuma combinação funcionou bem
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

        # 1. Coleta arquivos
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
            return

        print(f"Total de arquivos para unir: {len(arquivos_para_unir)}")

        # 2. Define output
        try:
            if not output_path:
                nome_arquivo = f"MASTER_CONSOLIDADO_{mes_alvo.replace('.','_')}.csv"
                output_path = os.path.join(root_path, nome_arquivo)

            # remove arquivo antigo
            if os.path.exists(output_path):
                os.remove(output_path)

            escreveu_algo = False
            total_linhas = 0
            total_arquivos_ok = 0

            # 3. Streaming write
            for arq in arquivos_para_unir:
                try:
                    df = self._tenta_ler_csv(arq)

                    # proteção contra vazio
                    if df is None or df.empty:
                        print(f"Arquivo vazio ignorado: {os.path.basename(arq)}")
                        continue

                    # colunas de rastreio
                    df["Origem_Arquivo"] = os.path.basename(arq)
                    df["Origem_Relatorio"] = os.path.basename(os.path.dirname(arq))

                    # grava streaming
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
                    print(f"Erro ao ler arquivo {os.path.basename(arq)}: {e}")
                    continue

            # 4. Resultado final
            if escreveu_algo:
                print("\nMASTER gerado com sucesso")
                print(f"Arquivo: {output_path}")
                print(f"Arquivos processados: {total_arquivos_ok}")
                print(f"Total de linhas: {total_linhas:,}")
                return output_path
            else:
                print("Nenhum dado válido encontrado.")
                return None

        except Exception as e:
            print(f"Erro crítico ao gerar MASTER: {e}")
            return None

    def unificar_partes(self, pasta_alvo, output_path=None):
        """Unifica arquivos divididos (_pt1, _pt2) na mesma pasta."""
        if (
            not pasta_alvo
            or not os.path.exists(pasta_alvo)
            or not os.path.isdir(pasta_alvo)
        ):
            print(
                f"Aviso: Diretório inválido ou inacessível para unificação: {pasta_alvo}"
            )
            return

        try:
            grupos = {}
            for f in os.listdir(pasta_alvo):
                if f.lower().endswith(".csv"):
                    # Regex para identificar base do nome (remove _pt1, _pt02, etc)
                    base_name = re.sub(r"_pt\d+", "", f, flags=re.IGNORECASE)
                    grupos.setdefault(base_name, []).append(os.path.join(pasta_alvo, f))

            for base, arquivos in grupos.items():
                if len(arquivos) > 1:
                    dfs = []
                    for arq in sorted(arquivos):
                        try:
                            dfs.append(self._tenta_ler_csv(arq))
                        except Exception as e:
                            print(f"Erro ao ler fragmento para unificação '{arq}': {e}")

                    if dfs:
                        final = pd.concat(dfs, ignore_index=True)
                        nome_final = f"UNIFICADO_{base}"
                        # Garante extensão .csv
                        if not nome_final.lower().endswith(".csv"):
                            nome_final += ".csv"

                        final.to_csv(
                            os.path.join(pasta_alvo, nome_final),
                            sep=";",
                            index=False,
                            encoding="utf-8-sig",
                        )
        except Exception as e:
            print(f"Erro unificação: {e}")

    # --- LÓGICA DO RELATÓRIO DE COMPLETUDE (INSERIDA) ---
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

            # CLONAGEM DE MÚLTIPLAS COLUNAS NO MASTER
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
        self._data_processor = DataProcessor()  # Reutiliza leitor robusto

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

            # Fallback estrito para CSV usando o leitor robusto
            df = self._data_processor._tenta_ler_csv(caminho_master)
            self.master_data = {"Base": df}
            return self.master_data

        except Exception as e:
            self.master_data = None
            raise Exception(
                f"Falha crítica ao carregar arquivo master '{caminho_master}': {str(e)}"
            )

    def validar_tabela(self, caminho_tabela, nome_aba_master=None, nome_tabela=None):
        """Valida a estrutura da tabela utilizando leituras tolerantes a falhas."""
        try:
            print(f"Validando tabela: {caminho_tabela}")

            # 1. Leitura Robusta da Tabela Alvo (Lê 5 linhas para garantir extração do cabeçalho real)
            if caminho_tabela.lower().endswith(".csv"):
                df_tabela = self._data_processor._tenta_ler_csv(caminho_tabela, nrows=5)
            else:
                df_tabela = pd.read_excel(caminho_tabela, nrows=5)

            if df_tabela is None or df_tabela.empty or len(df_tabela.columns) == 0:
                return False, "Tabela inválida ou vazia."

            colunas_tabela = set(df_tabela.columns.astype(str).str.strip().str.upper())

            # 2. Resolução do Master
            if self.master_data is None:
                if not self.master_path:
                    self.master_path = self._encontrar_arquivo_master()
                    if not self.master_path:
                        return (
                            False,
                            "ARQUIVO MASTER INACESSÍVEL: Validação abortada para evitar falso-positivos.",
                        )

                self.carregar_master(self.master_path)

            # 3. Mapeamento
            modelo_correto = self._encontrar_modelo_no_master(
                nome_aba_master, nome_tabela, colunas_tabela
            )

            if not modelo_correto:
                return (
                    False,
                    "Nenhum modelo de dados compatível foi encontrado no arquivo Master.",
                )

            # 4. Cálculo de Conformidade
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

        # Aceita o match se bater pelo menos 5% (Evita falhas por espaços ou caracteres especiais)
        if melhor_match and maior_similaridade > 0.05:
            return melhor_match

        return None

    def _extrair_colunas_master_candidatas(self, df_master):
        """RAIO-X: Procura o verdadeiro cabeçalho mesmo que ele não esteja na primeira linha"""
        candidatas = []
        dfs_para_analisar = [df_master]

        # Tenta achar cabeçalhos deslocados nas primeiras 20 linhas (Muito comum no CMDB)
        for i in range(min(20, len(df_master))):
            linha_str = df_master.iloc[i].astype(str).str.lower()
            # Se encontrar palavras chaves do CMDB na linha, assume que é o cabeçalho verdadeiro!
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
            # Tenta encontrar colunas verticais de dicionário
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
                    # Filtra células vazias ("") que podem virar colunas fantasmas
                    colunas = set(serie_limpa[serie_limpa != ""])
                    if colunas:
                        candidatas.append(colunas)

            # Sempre avalia o cabeçalho natural da tabela em questão
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
        self.carregar_master(
            caminho_master, force_reload=True
        )  # Força recarregamento na próxima validação


# Função de conveniência para uso rápido
def validar_estrutura_tabela(caminho_tabela, caminho_master=None, nome_aba=None):
    """
    Função rápida para validar uma tabela contra o master

    Exemplo:
        sucesso, resultado = validar_estrutura_tabela("dados.csv")
    """
    validador = StructureValidator()
    if caminho_master:
        validador.definir_master_manual(caminho_master)

    return validador.validar_tabela(caminho_tabela, nome_aba)
