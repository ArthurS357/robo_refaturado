import time
import os
import subprocess
import ctypes
import shutil
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException


class BrowserController:
    def __init__(self):
        self.driver = None
        self.debug_port = "9222"
        self.user_data_dir = r"C:\ChromeDebug"

    def abrir_chrome_debug(self):
        """
        Abre o Chrome em modo de depuração remota.
        Retorna: (Sucesso: bool, Mensagem: str)
        """
        # 1. Tenta fechar instâncias antigas
        try:
            os.system("taskkill /f /im chrome.exe >nul 2>&1")
            time.sleep(1)
        except:
            pass

        # 2. Garante que a pasta de perfil existe
        if not os.path.exists(self.user_data_dir):
            try:
                os.makedirs(self.user_data_dir)
            except OSError as e:
                return False, f"Erro ao criar pasta de perfil: {e}"

        # 3. Localiza o executável do Chrome
        caminhos_possiveis = [
            r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
            os.path.expanduser(r"~\AppData\Local\Google\Chrome\Application\chrome.exe"),
            os.path.expanduser(
                r"~\Local Settings\Application Data\Google\Chrome\Application\chrome.exe"
            ),
        ]

        chrome_path = next((c for c in caminhos_possiveis if os.path.exists(c)), None)

        if not chrome_path:
            if shutil.which("chrome"):
                chrome_path = "chrome"
            else:
                return False, "Executável do Chrome não encontrado."

        # 4. Monta os argumentos
        args = [
            chrome_path,
            f"--remote-debugging-port={self.debug_port}",
            f"--user-data-dir={self.user_data_dir}",
            "--no-first-run",
            "--no-default-browser-check",
            "--remote-allow-origins=*",
            "about:blank",
        ]

        try:
            subprocess.Popen(args)
            return True, "Chrome aberto em modo Debug."
        except Exception as e:
            return False, f"Erro ao iniciar processo: {e}"

    def conectar(self):
        """Conecta o Selenium à instância do Chrome aberta."""
        if self.driver:
            self.desconectar()

        opt = webdriver.ChromeOptions()
        opt.add_experimental_option("debuggerAddress", f"127.0.0.1:{self.debug_port}")
        opt.add_argument("--remote-allow-origins=*")

        try:
            try:
                service = Service(ChromeDriverManager().install())
            except:
                service = Service()

            self.driver = webdriver.Chrome(service=service, options=opt)

            try:
                self.driver.set_window_position(0, 0)
                self.driver.maximize_window()
            except:
                pass

            time.sleep(0.5)
            return True
        except WebDriverException as e:
            print(f"Erro WebDriver ao conectar: {e}")
            return False
        except Exception as e:
            print(f"Erro genérico ao conectar: {e}")
            return False

    def desconectar(self):
        try:
            if self.driver:
                self.driver = None
        except:
            pass

    def navegar(self, link):
        if not self.driver:
            return False
        try:
            self.driver.get(link)
            WebDriverWait(self.driver, 60).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            return True
        except WebDriverException as e:
            print(f"Erro WebDriver ao navegar: {e}")
            return False
        except Exception:
            return False

    # --- FUNÇÕES DE ESPERA INTELIGENTE ---

    def esperar_elemento(self, seletor, by=By.XPATH, timeout=20):
        if not self.driver:
            return False
        try:
            WebDriverWait(self.driver, timeout).until(
                EC.visibility_of_element_located((by, seletor))
            )
            return True
        except TimeoutException:
            return False
        except WebDriverException:
            return False

    def esperar_loading_sumir(self, seletor_loading, by=By.XPATH, timeout=20):
        if not self.driver:
            return True
        try:
            if not self.driver.find_elements(by, seletor_loading):
                return True

            WebDriverWait(self.driver, timeout).until(
                EC.invisibility_of_element_located((by, seletor_loading))
            )
            return True
        except (TimeoutException, WebDriverException):
            return False

    # -------------------------------------------

    def verificar_vazio(self):
        if not self.driver:
            return False
        try:
            body = self.driver.find_element(By.TAG_NAME, "body")
            txt = body.text.lower()
            termos = [
                "no records",
                "não há registros",
                "nenhum registro",
                "no rows",
                "0 items",
                "no data available",
                "nenhum dado encontrado",
            ]
            return any(t in txt for t in termos)
        except WebDriverException:
            return False

    def clique_inteligente(self, xpath=None, x_fisico=0, y_fisico=0, botao="left"):
        # Converte para int seguro
        try:
            cx, cy = int(float(x_fisico)), int(float(y_fisico))
        except:
            cx, cy = 0, 0

        # 1. Prioridade: Clique Físico
        if cx > 0 and cy > 0:
            self._clique_fisico_ctypes(cx, cy, botao)
            return

        # 2. Fallback: Clique Selenium
        if xpath and self.driver:
            try:
                elem = WebDriverWait(self.driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, xpath))
                )
                if botao == "right":
                    webdriver.ActionChains(self.driver).context_click(elem).perform()
                else:
                    elem.click()
                time.sleep(0.5)
            except WebDriverException as e:
                print(f"Falha no clique Selenium ({xpath}): {e}")
            except Exception:
                pass

    def clique_hibrido(self, x, y):
        self._clique_fisico_ctypes(x, y, "left")

    def clique_direito(self, x, y):
        self._clique_fisico_ctypes(x, y, "right")

    def _clique_fisico_ctypes(self, x, y, botao="left"):
        try:
            x, y = int(float(x)), int(float(y))
            if x <= 0 and y <= 0:
                return

            ctypes.windll.user32.SetCursorPos(x, y)
            time.sleep(0.1)

            if botao == "left":
                down, up = 0x0002, 0x0004
            else:
                down, up = 0x0008, 0x0010

            ctypes.windll.user32.mouse_event(down, 0, 0, 0, 0)
            time.sleep(0.05)
            ctypes.windll.user32.mouse_event(up, 0, 0, 0, 0)

        except Exception as e:
            print(f"Erro no clique físico: {e}")

    def pegar_posicao_mouse(self):
        class P(ctypes.Structure):
            _fields_ = [("x", ctypes.c_long), ("y", ctypes.c_long)]

        pt = P()
        ctypes.windll.user32.GetCursorPos(ctypes.byref(pt))
        return pt.x, pt.y
