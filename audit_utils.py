import json
import csv
import subprocess
import tkinter as tk
from pathlib import Path
from datetime import datetime, timedelta


class ConfigManager:
    def __init__(self):
        # Define a pasta de Downloads usando Pathlib
        self.pasta_downloads = Path.home() / "Downloads"
        self.config_file = self.pasta_downloads / "audit_config.json"

    def carregar(self):
        """Carrega as configurações do arquivo JSON se ele existir."""
        if self.config_file.exists():
            try:
                # 'utf-8' é essencial para evitar erros com caracteres especiais
                with self.config_file.open("r", encoding="utf-8") as f:
                    dados = json.load(f)
                    # Validação simples para garantir que retornamos um dicionário
                    if isinstance(dados, dict):
                        return dados
                    else:
                        print(
                            f"[ConfigManager] Erro: Arquivo de configuração inválido (não é um dicionário)."
                        )
                        return {}
            except (json.JSONDecodeError, OSError) as e:
                print(f"[ConfigManager] Erro ao carregar configuração: {e}")
                return {}
        return {}

    def salvar(self, dados):
        """Salva o dicionário de configurações no arquivo JSON."""
        try:
            # 1. Garante que a pasta existe antes de tentar salvar
            self.config_file.parent.mkdir(parents=True, exist_ok=True)

            # 2. Salva com encoding utf-8
            with self.config_file.open("w", encoding="utf-8") as f:
                json.dump(dados, f, indent=4)
            return True
        except OSError as e:
            print(f"[ConfigManager] Erro ao salvar config: {e}")
            return False


class LogManager:
    def __init__(self):
        # Ajustei o cabeçalho para incluir Tempo na coluna 2 (antiga "Parte")
        self.cabecalho = ["Modulo", "Tempo", "Status", "Linhas", "Data", "Link"]
        self.historico_links = set()
        self.sessao_atual = []

    def carregar_historico(self, caminho_csv):
        """Lê um histórico CSV existente para evitar reprocessamento."""
        path = Path(caminho_csv)
        count = 0
        if not path.exists():
            return 0

        try:
            with path.open("r", encoding="utf-8", errors="ignore") as f:
                leitor = csv.reader(f, delimiter=";")  # Padronizado delimiter ;
                try:
                    next(leitor, None)  # Pula cabeçalho
                except StopIteration:
                    pass

                for linha in leitor:
                    # Verifica se a linha é válida e se o status indica sucesso
                    if len(linha) >= 6:
                        status_str = linha[2].lower()
                        if any(
                            x in status_str
                            for x in ["ok", "sucesso", "pulado", "concluído"]
                        ):
                            link_val = linha[5].strip()
                            if link_val:
                                self.historico_links.add(link_val)  # Coluna do Link
                                count += 1
        except Exception as e:
            print(f"[LogManager] Erro ao carregar histórico: {e}")
        return count

    def verificar_processado(self, link):
        return link in self.historico_links

    def registrar(self, nome, status, linhas, link, tempo=""):
        """
        Agora aceita o argumento 'tempo'.
        """
        row = [
            nome,
            str(tempo),  # Armazena o tempo aqui
            status,
            str(linhas),
            datetime.now().strftime("%d/%m %H:%M"),
            link,
        ]
        self.sessao_atual.append(row)

    def exportar_sessao(self):
        nome = f"Relatorio_Execucao_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        path = Path.home() / "Downloads" / nome
        try:
            # Uso utf-8-sig para o Excel reconhecer acentuação automaticamente
            with path.open("w", newline="", encoding="utf-8-sig") as f:
                w = csv.writer(f, delimiter=";")  # Delimitador ; é melhor para Excel BR
                w.writerow(self.cabecalho)
                w.writerows(self.sessao_atual)
        except Exception as e:
            print(f"[LogManager] Erro ao exportar sessão: {e}")
        return str(path)


class MaintenanceTool:
    """Nova classe para limpeza e manutenção de arquivos temporários."""

    @staticmethod
    def limpar_logs_antigos(dias=7):
        """Remove relatórios HTML e CSVs da pasta Downloads mais velhos que X dias."""
        pasta_downloads = Path.home() / "Downloads"
        removidos = 0

        # Padrões de arquivos gerados pelo sistema
        padroes = [
            "Dashboard_Audit_*.html",
            "Relatorio_Execucao_*.csv",
            "*.tmp",
            "*.crdownload",
        ]
        limite = datetime.now() - timedelta(days=dias)

        for padrao in padroes:
            # Pathlib globbing
            for arquivo in pasta_downloads.glob(padrao):
                try:
                    mtime = datetime.fromtimestamp(arquivo.stat().st_mtime)
                    if mtime < limite:
                        arquivo.unlink()  # Delete
                        removidos += 1
                except OSError as e:
                    print(f"[MaintenanceTool] Erro ao remover {arquivo.name}: {e}")
        return removidos


class SystemNotifier:
    @staticmethod
    def enviar_notificacao(titulo, mensagem):
        """Envia uma notificação Toast do Windows usando PowerShell."""
        ps_script = f"""
        $code = {{
            [Windows.UI.Notifications.ToastNotificationManager, Windows.UI.Notifications, ContentType = WindowsRuntime] > $null;
            $template = [Windows.UI.Notifications.ToastNotificationManager]::GetTemplateContent([Windows.UI.Notifications.ToastTemplateType]::ToastText02);
            $textNodes = $template.GetElementsByTagName("text");
            $textNodes.Item(0).AppendChild($template.CreateTextNode("{titulo}")) > $null;
            $textNodes.Item(1).AppendChild($template.CreateTextNode("{mensagem}")) > $null;
            $notifier = [Windows.UI.Notifications.ToastNotificationManager]::CreateToastNotifier("Robô Audit");
            $notification = [Windows.UI.Notifications.ToastNotification, Windows.UI.Notifications, ContentType = WindowsRuntime]::new($template);
            $notifier.Show($notification);
        }}
        Invoke-Command -ScriptBlock $code
        """
        try:
            subprocess.Popen(
                ["powershell", "-Command", ps_script],
                creationflags=subprocess.CREATE_NO_WINDOW,
            )
        except Exception as e:
            print(f"[SystemNotifier] Erro ao enviar notificação: {e}")


# --- IMPLEMENTAÇÃO DO POPUP DE CONTAGEM ---


def exibir_contador_inicio(parent, callback_funcao_robo):
    """
    Exibe um popup centralizado com contagem regressiva (3, 2, 1).
    Usa .after() para não travar a interface gráfica.
    Ao chegar em 0, fecha o popup e dispara a função do robô (callback).
    """
    # 1. Configurar Janela Popup
    top = tk.Toplevel(parent)
    top.title("")
    top.attributes("-topmost", True)  # Sempre no topo
    top.overrideredirect(True)  # Remove bordas e barra de título
    top.config(bg="#f0f0f0", relief="raised", bd=3)

    # 2. Centralizar Janela na Tela (baseado na janela pai)
    largura = 300
    altura = 200

    # Tenta pegar as coordenadas da janela pai, senão usa o screen geral
    try:
        x_pai = parent.winfo_rootx()
        y_pai = parent.winfo_rooty()
        largura_pai = parent.winfo_width()
        altura_pai = parent.winfo_height()

        x_pos = x_pai + (largura_pai // 2) - (largura // 2)
        y_pos = y_pai + (altura_pai // 2) - (altura // 2)
    except:
        # Fallback se não conseguir ler o pai
        x_pos = (top.winfo_screenwidth() // 2) - (largura // 2)
        y_pos = (top.winfo_screenheight() // 2) - (altura // 2)

    top.geometry(f"{largura}x{altura}+{x_pos}+{y_pos}")

    # 3. Widgets (Texto)
    lbl_msg = tk.Label(
        top, text="O Robô iniciará em:", font=("Segoe UI", 12), bg="#f0f0f0"
    )
    lbl_msg.pack(pady=(20, 10))

    lbl_cont = tk.Label(
        top, text="3", font=("Segoe UI", 60, "bold"), fg="#D32F2F", bg="#f0f0f0"
    )
    lbl_cont.pack()

    # 4. Lógica de Contagem (Recursiva com .after para não travar)
    def atualizar_relogio(segundos):
        if segundos > 0:
            lbl_cont.config(text=str(segundos))
            # Agenda a próxima execução para daqui a 1000ms (1 segundo)
            top.after(1000, lambda: atualizar_relogio(segundos - 1))
        else:
            top.destroy()  # Fecha o popup
            parent.update()  # Garante que a tela limpe o popup visualmente

            # Dispara o robô se houver callback
            if callback_funcao_robo:
                callback_funcao_robo()

    # Inicia o ciclo
    atualizar_relogio(3)
