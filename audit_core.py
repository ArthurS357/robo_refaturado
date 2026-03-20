import time
import shutil
import ctypes
import json  # Adicionado para manipular os metadados
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Callable, Optional, Set
from threading import Event, Lock  # THREAD-SAFE UPDATE

# Imports do Selenium (No topo para performance)
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

# Importação dos Módulos Refatorados
try:
    from audit_browser import BrowserController
    from audit_data import DataProcessor
    from audit_utils import ConfigManager, LogManager
except ImportError as e:
    raise ImportError(f"Faltam módulos refatorados: {e}")


# Alias para manter compatibilidade com a UI
class GerenciadorLog(LogManager):
    def importar_historico(self, caminho):
        return self.carregar_historico(caminho)

    def verificar_ja_feito(self, link):
        return self.verificar_processado(link)

    def registrar_acao(self, nome, status, linhas, link):
        self.registrar(nome, status, linhas, link)

    def salvar_sessao_csv(self):
        return self.exportar_sessao()


class AuditMotor:
    def __init__(self):
        # THREAD-SAFE UPDATE: Substituir booleanos por Events
        self.rodando_event = Event()
        self.pausado_event = Event()

        # Subsistemas
        self.browser_ctrl: Optional[BrowserController] = None
        self.data_processor = DataProcessor()
        self.config_manager = ConfigManager()
        self.config = self.config_manager.carregar()

        # THREAD-SAFE UPDATE: Adicionar lock para metadados
        self._metadata_lock = Lock()

        # Pathlib
        self.pasta_downloads = Path(self.config_manager.pasta_downloads)
        self.driver = None

    # --- CONFIGURAÇÃO ---
    def carregar_config(self):
        return self.config_manager.carregar()

    def salvar_config(self, dados):
        self.config = dados
        return self.config_manager.salvar(dados)

    # --- CONTROLE DE FLUXO ---
    # THREAD-SAFE UPDATE: Versão production-safe usando Events
    def check_status(self) -> bool:
        """
        Verifica se o robô deve continuar executando e gerencia pausa
        de forma thread-safe e responsiva.
        """

        # Se recebeu comando de parar, encerra imediatamente
        if not self.rodando_event.is_set():
            return False

        # Se estiver pausado, entra em modo de espera controlado
        while self.pausado_event.is_set():

            # Permite sair imediatamente se parar durante pausa
            if not self.rodando_event.is_set():
                return False

            # Pequeno delay para evitar consumo excessivo de CPU
            time.sleep(0.1)

        return True

    # THREAD-SAFE UPDATE: Refatorar para usar Events
    def toggle_pause(self):
        if self.pausado_event.is_set():
            self.pausado_event.clear()
        else:
            self.pausado_event.set()

    # THREAD-SAFE UPDATE: Refatorar para usar Events
    def parar(self):
        self.rodando_event.clear()
        self.pausado_event.clear()

    # --- NAVEGAÇÃO E DRIVER ---
    def conectar_driver(self) -> bool:
        """Reinicia o controller para garantir um driver limpo."""
        try:
            self.browser_ctrl = BrowserController()
            sucesso = self.browser_ctrl.conectar()
            self.driver = self.browser_ctrl.driver
            return sucesso
        except Exception as e:
            print(f"Erro ao conectar driver: {e}")
            return False

    def abrir_navegador_debug(self):
        temp_browser = BrowserController()
        return temp_browser.abrir_chrome_debug()

    def navegar_seguro(self, link: str) -> bool:
        if not self.check_status():
            return False
        if self.browser_ctrl:
            return self.browser_ctrl.navegar(link)
        return False

    def verificar_vazio(self) -> bool:
        if self.browser_ctrl:
            return self.browser_ctrl.verificar_vazio()
        return False

    # --- INTERAÇÃO FÍSICA ---
    def pegar_mouse_pos(self):
        """Retorna a posição atual (x, y) do mouse usando Windows API."""

        class POINT(ctypes.Structure):
            _fields_ = [("x", ctypes.c_long), ("y", ctypes.c_long)]

        pt = POINT()
        try:
            ctypes.windll.user32.GetCursorPos(ctypes.byref(pt))
            return pt.x, pt.y
        except Exception as e:
            print(f"Erro ao pegar mouse: {e}")
            return 0, 0

    def clique_fisico(self, x, y, botao="esquerdo"):
        ctrl = self.browser_ctrl if self.browser_ctrl else BrowserController()
        if botao == "direito":
            ctrl.clique_direito(x, y)
        else:
            ctrl.clique_hibrido(
                x, y
            )  # Híbrido é mais seguro (move físico, clica driver se possível)

    # --- PROCESSAMENTO DE DADOS ---
    def contar_linhas_csv(self, path):
        return self.data_processor.contar_linhas(path)

    def scan_base_dados(self, root_path, filtro_dias):
        return self.data_processor.listar_historico(root_path, filtro_dias)

    def processar_fusao_partes(self, pasta, output_path=None):
        return self.data_processor.unificar_partes(pasta, output_path=output_path)

    def processar_master(self, root_path, mes_alvo, output_path=None):
        return self.data_processor.gerar_master(
            root_path, mes_alvo, output_path=output_path
        )

    # --- VERIFICAÇÃO DE EXISTÊNCIA (NOVO) ---
    def verificar_existencia_dados(self, item_name, path_destino, mes_ref):
        """
        Verifica se os dados existem e retorna informações detalhadas.
        """
        dir_final = Path(path_destino) / mes_ref
        arquivo_csv = dir_final / f"{item_name}.csv"
        arquivo_vazio = dir_final / f"{item_name} - Não há base de dados.txt"

        existe_csv = (
            arquivo_csv.exists() and arquivo_csv.stat().st_size > 100
        )  # mínimo 100 bytes
        existe_vazio = arquivo_vazio.exists()

        status_existencia = {
            "existe_csv": existe_csv,
            "existe_vazio": existe_vazio,
            "tamanho_csv": arquivo_csv.stat().st_size if existe_csv else 0,
            "caminho": str(dir_final),
        }

        return status_existencia

    # THREAD-SAFE UPDATE: Proteger com Lock
    def _registrar_metadados_existencia(self, iid, item, status_existencia, tipo):
        """Registra metadados detalhados sobre a existência dos dados."""
        with self._metadata_lock:  # THREAD-SAFE UPDATE: Adicionar lock
            metadados = {
                "iid": iid,
                "nome": item["name"],
                "link": item["link"],
                "timestamp": datetime.now().isoformat(),
                "tipo": tipo,
                "existe_csv": status_existencia["existe_csv"],
                "existe_vazio": status_existencia["existe_vazio"],
                "tamanho_arquivo": status_existencia["tamanho_csv"],
                "caminho": status_existencia["caminho"],
            }

            # Armazena em um arquivo de metadados do robô na pasta de downloads (área de trabalho temporária)
            metadados_path = (
                Path(self.pasta_downloads) / "audit_metadados_existencia.json"
            )

            try:
                dados_existentes = []
                if metadados_path.exists():
                    with open(metadados_path, "r", encoding="utf-8") as f:
                        try:
                            dados_existentes = json.load(f)
                        except json.JSONDecodeError:
                            dados_existentes = []

                # Atualiza ou adiciona o registro
                encontrado = False
                for i, registro in enumerate(dados_existentes):
                    if (
                        registro.get("iid") == iid
                        and registro.get("nome") == item["name"]
                    ):
                        dados_existentes[i] = metadados
                        encontrado = True
                        break

                if not encontrado:
                    dados_existentes.append(metadados)

                with open(metadados_path, "w", encoding="utf-8") as f:
                    json.dump(dados_existentes, f, indent=2, ensure_ascii=False)

            except Exception as e:
                print(f"Erro ao salvar metadados: {e}")

    # --- LÓGICA CORE (EXECUTOR) ---
    def executar_fila(
        self,
        lista_itens: List[Dict],
        callbacks: Dict[str, Callable],
        pular_existentes=False,
    ):
        """
        Loop Principal de Execução.
        """
        # THREAD-SAFE UPDATE: Inicializar Events
        self.rodando_event.set()
        self.pausado_event.clear()

        mes_atual = datetime.now().strftime("%m.%Y")

        # Callbacks seguros
        cb_status = callbacks.get("on_status", lambda i, s, t, tm, l: None)
        cb_log = callbacks.get("on_log", lambda m: print(m))
        cb_progress = callbacks.get("on_progress", lambda v: None)

        # Validação Inicial
        if not self.browser_ctrl or not self.driver:
            if not self.conectar_driver():
                cb_log("Erro Crítico: Não foi possível conectar ao Chrome.")
                self.rodando_event.clear()  # THREAD-SAFE UPDATE: Usar Event
                return

        total_itens = len(lista_itens)
        cb_log(f"Iniciando fila com {total_itens} itens.")

        for idx, item in enumerate(lista_itens):
            if not self.check_status():
                break

            cb_progress(idx + 1)

            # Delega o processamento de um único item
            self._processar_item(item, mes_atual, callbacks, pular_existentes)

        self.rodando_event.clear()  # THREAD-SAFE UPDATE: Usar Event
        cb_log("Execução Finalizada.")

    def _processar_item(
        self, item: Dict, mes_atual: str, callbacks: Dict, pular_existentes: bool
    ):
        """Processa um único item da fila (Navegar -> Baixar -> Mover)."""

        # Extração de Callbacks e Configs
        cb_status = callbacks.get("on_status", lambda i, s, t, tm, l: None)
        cb_log = callbacks.get("on_log", lambda m: print(m))

        xpath_wait = self.config.get("xpath_wait", "")

        # Dados do Item
        iid = item["iid"]
        nome = item["name"]
        link = item["link"]
        path_destino_raiz = Path(item["path"])

        # Caminhos
        dir_mes = path_destino_raiz / mes_atual
        caminho_final = dir_mes / f"{nome}.csv"

        start_time = time.time()

        # 0. Verificação Inicial de Existência
        status_existencia = self.verificar_existencia_dados(
            nome, path_destino_raiz, mes_atual
        )

        # 1. Verifica Existência (Pular se existir)
        if pular_existentes and status_existencia["existe_csv"]:
            cb_status(iid, "Pulado (Existe)", ("run",), "-", "-")
            cb_log(f"-> Pulado: {nome} (dados existentes)")
            return

        cb_status(iid, "Acessando...", ("run",), "...", "-")
        cb_log(f"Processando: {nome}")

        # 2. Navegação
        if not self.navegar_seguro(link):
            self._reportar_erro(iid, "Erro Link", start_time, cb_status)
            return

        # 3. Espera Inteligente (Smart Wait)
        cb_status(iid, "Carregando...", ("run",), "...", "-")
        self._aguardar_carregamento(xpath_wait)

        # 4. Verificar Vazio
        # Verifica se o site mostra vazio visualmente OU se já temos histórico de vazio
        if self.verificar_vazio() or status_existencia["existe_vazio"]:
            tempo_vazio = f"{int(time.time() - start_time)}s"
            cb_status(iid, "Vazio", ("empty",), tempo_vazio, "0")

            # Gera o txt se ainda não existir
            if not status_existencia["existe_vazio"]:
                self.gerar_arquivo_vazio(path_destino_raiz, nome, mes_atual)

            # Atualiza status e registra metadados
            novo_status = self.verificar_existencia_dados(
                nome, path_destino_raiz, mes_atual
            )
            self._registrar_metadados_existencia(iid, item, novo_status, "vazio")
            return

        # 5. Tentativa de Download
        arquivo_baixado = self._tentar_download(iid, cb_status, start_time)

        if arquivo_baixado:
            # 6. Finalização (Mover e Contar)
            cb_status(iid, "Processando...", ("run",), "...", "-")
            sucesso, msg_ou_linhas = self._finalizar_arquivo(
                arquivo_baixado, caminho_final, dir_mes, callbacks
            )

            tempo_total = f"{int(time.time() - start_time)}s"

            if sucesso:
                cb_status(iid, "Concluído", ("ok",), tempo_total, str(msg_ou_linhas))
                cb_log(f"-> Sucesso: {nome} ({msg_ou_linhas} linhas)")

                # Registra sucesso nos metadados
                status_final = self.verificar_existencia_dados(
                    nome, path_destino_raiz, mes_atual
                )
                self._registrar_metadados_existencia(iid, item, status_final, "sucesso")
            else:
                cb_status(iid, "Erro Arquivo", ("err",), tempo_total, "-")
                cb_log(f"Erro ao finalizar {nome}: {msg_ou_linhas}")
        else:
            # Erro já reportado dentro de _tentar_download ou timeout
            if (
                self.check_status()
            ):  # Só reporta timeout se não foi cancelado pelo usuário
                self._reportar_erro(iid, "Timeout DL", start_time, cb_status)

    def _aguardar_carregamento(self, xpath_custom: str):
        """Espera carregamento da página e spinners."""
        if xpath_custom:
            try:
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.XPATH, xpath_custom))
                )
            except TimeoutException:
                pass

        spinners = ["//div[contains(@class, 'load')]", "//div[contains(@id, 'load')]"]
        for sp in spinners:
            try:
                WebDriverWait(self.driver, 5).until(
                    EC.invisibility_of_element_located((By.XPATH, sp))
                )
            except TimeoutException:
                pass

    def _tentar_download(self, iid, cb_status, start_time) -> Optional[Path]:
        """Gerencia a interação física e o monitoramento do download."""

        coords = self.config.get("coords", {})
        timeout_btn = int(self.config.get("timeout_retry_btn", 300))
        timeout_dl = int(self.config.get("timeout_dl", 200))

        # Cliques Preparatórios
        botoes_pre = [
            ("btn_rclick", "direito"),
            ("btn_exp", "esquerdo"),
            ("btn_csv", "esquerdo"),
        ]

        # Tira "foto" da pasta antes de clicar
        snapshot = set(self.pasta_downloads.glob("*"))

        for key, tipo_clique in botoes_pre:
            if not self.check_status():
                return None
            c_val = coords.get(key, "0,0")
            if "," in c_val:
                cx, cy = c_val.split(",")
                self.clique_fisico(cx, cy, tipo_clique)
                time.sleep(1.0)  # Delay UI

        # Loop de Clique no Botão de Download
        cb_status(iid, "Baixando...", ("run",), "...", "-")

        dl_key = coords.get("btn_dl", "0,0")
        dx, dy = dl_key.split(",") if "," in dl_key else (0, 0)

        download_iniciado = False
        tempo_tentativa = 0

        while tempo_tentativa < timeout_btn:
            if not self.check_status():
                return None

            self.clique_fisico(dx, dy, "esquerdo")

            if self.esperar_download_inicio_pathlib(snapshot, 2):
                download_iniciado = True
                break

            if tempo_tentativa > 5:
                cb_status(
                    iid, f"Tentando ({tempo_tentativa}s)...", ("run",), "...", "-"
                )

            time.sleep(1)
            tempo_tentativa += 3  # Tenta a cada 3 segundos + tempo de exec

        if download_iniciado:
            cb_status(iid, "Aguardando Fim...", ("run",), "...", "-")
            return self.esperar_download_fim_pathlib(snapshot, timeout_dl)

        return None

    def _finalizar_arquivo(
        self, arquivo_origem: Path, caminho_final: Path, dir_mes: Path, callbacks: Dict
    ):
        """Move o arquivo, trata conflitos e conta linhas."""
        cb_ask = callbacks.get("ask_duplicate", lambda p: "copia")
        clean_tmp = self.config.get("clean_after", False)

        try:
            dir_mes.mkdir(parents=True, exist_ok=True)

            if caminho_final.exists():
                acao = cb_ask(str(caminho_final))  # User interaction
                if acao == "substituir":
                    try:
                        caminho_final.unlink()
                    except OSError:
                        pass
                elif acao == "copia":
                    ts = datetime.now().strftime("%H%M%S")
                    caminho_final = dir_mes / f"{caminho_final.stem}_Copia_{ts}.csv"
                else:
                    return False, "Cancelado pelo usuário"

            shutil.move(str(arquivo_origem), str(caminho_final))

            if clean_tmp:
                self._limpar_temporarios()

            qtd_linhas = self.contar_linhas_csv(str(caminho_final))
            return True, qtd_linhas

        except Exception as e:
            return False, str(e)

    def _reportar_erro(self, iid, msg, start_time, cb_status):
        tempo = f"{int(time.time() - start_time)}s"
        cb_status(iid, msg, ("err",), tempo, "-")

    def _limpar_temporarios(self):
        for tmp in self.pasta_downloads.glob("*.tmp"):
            try:
                tmp.unlink()
            except:
                pass

    # --- AUXILIARES PATHLIB ---
    def esperar_download_inicio_pathlib(
        self, snapshot_set: Set[Path], timeout: float
    ) -> bool:
        fim = time.time() + timeout
        while time.time() < fim:
            if not self.check_status():
                return False
            try:
                atual = set(self.pasta_downloads.glob("*"))
                if len(atual - snapshot_set) > 0:
                    return True
            except:
                pass
            time.sleep(0.5)
        return False

    def esperar_download_fim_pathlib(
        self, snapshot_set: Set[Path], timeout: float
    ) -> Optional[Path]:
        fim = time.time() + timeout
        while time.time() < fim:
            if not self.check_status():
                return None
            try:
                atual = set(self.pasta_downloads.glob("*"))
                novos = atual - snapshot_set

                # Verifica se ainda tem arquivo baixando (.crdownload, .tmp, .part)
                downloads_ativos = [
                    f for f in atual if f.suffix in [".crdownload", ".tmp", ".part"]
                ]

                # Candidatos são arquivos novos que NÃO são temporários
                candidatos = [
                    f for f in novos if f.suffix not in [".crdownload", ".tmp", ".part"]
                ]

                if candidatos and not downloads_ativos:
                    # Pega o arquivo mais recente entre os novos
                    arquivo = max(candidatos, key=lambda p: p.stat().st_mtime)

                    # Espera estabilização do tamanho do arquivo
                    tamanho_inicial = arquivo.stat().st_size
                    time.sleep(1)
                    if arquivo.stat().st_size == tamanho_inicial:
                        return arquivo
            except:
                pass
            time.sleep(1)
        return None

    def gerar_arquivo_vazio(self, path_destino, nome_item, mes_ref):
        try:
            dir_final = path_destino / mes_ref
            dir_final.mkdir(parents=True, exist_ok=True)
            arquivo_txt = dir_final / f"{nome_item} - Não há base de dados.txt"
            texto = (
                f"Verificado em: {datetime.now().strftime('%d/%m/%Y %H:%M')}\n"
                f"Item auditado: {nome_item}\n"
                "Status: Vazio (Sem registros encontrados no sistema)\n"
                "Gerado automaticamente pelo Robô Audit."
            )
            arquivo_txt.write_text(texto, encoding="utf-8")
            return True
        except Exception as e:
            print(f"Erro ao gerar txt vazio: {e}")
            return False
