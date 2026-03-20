import os
from datetime import datetime


class AuditReporter:
    def __init__(self):
        # CSS Estilo Clean / Dashboard Moderno
        self.css = """
            body { font-family: 'Segoe UI', sans-serif; background-color: #f3f4f6; color: #1f2937; margin: 0; padding: 20px; }
            .container { max-width: 1000px; margin: 0 auto; background: white; padding: 30px; border-radius: 12px; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.1); }
            h1 { color: #111827; border-bottom: 2px solid #e5e7eb; padding-bottom: 10px; font-size: 1.5rem; }
            .summary { display: grid; grid-template-columns: repeat(4, 1fr); gap: 20px; margin-bottom: 30px; }
            .card { background: #f9fafb; padding: 20px; border-radius: 8px; text-align: center; border: 1px solid #e5e7eb; transition: transform 0.2s; }
            .card:hover { transform: translateY(-2px); box-shadow: 0 4px 6px -1px rgba(0,0,0,0.05); }
            .card h3 { margin: 0; color: #6b7280; font-size: 0.8em; text-transform: uppercase; letter-spacing: 0.05em; }
            .card p { margin: 10px 0 0; font-size: 2em; font-weight: bold; color: #374151; }
            .success { color: #059669 !important; }
            .error { color: #dc2626 !important; }
            .empty { color: #d97706 !important; }
            
            .info-bar { margin-bottom: 20px; padding: 15px; background: #eef2ff; border-radius: 8px; color: #3730a3; border: 1px solid #c7d2fe; display: flex; justify-content: space-between; align-items: center; font-size: 0.9em; }
            
            table { width: 100%; border-collapse: collapse; margin-top: 20px; table-layout: fixed; }
            th, td { padding: 12px; text-align: left; border-bottom: 1px solid #e5e7eb; word-wrap: break-word; font-size: 0.9rem; }
            th { background-color: #f9fafb; color: #374151; font-weight: 600; text-transform: uppercase; font-size: 0.8rem; letter-spacing: 0.05em; }
            tr:hover { background-color: #f8fafc; }
            
            .tag { padding: 4px 8px; border-radius: 4px; font-size: 0.75em; font-weight: 600; display: inline-block; text-transform: uppercase; }
            .tag-ok { background: #d1fae5; color: #065f46; }
            .tag-err { background: #fee2e2; color: #991b1b; }
            .tag-warn { background: #fef3c7; color: #92400e; }
            .tag-info { background: #dbeafe; color: #1e40af; }
            
            a { color: #2563eb; text-decoration: none; transition: color 0.2s; }
            a:hover { text-decoration: underline; color: #1d4ed8; }
            
            .footer { margin-top: 40px; text-align: center; color: #9ca3af; font-size: 0.8em; border-top: 1px solid #e5e7eb; padding-top: 20px; }
            
            /* Barra de Progresso Visual */
            .progress-container { width: 100%; background-color: #e5e7eb; border-radius: 4px; height: 8px; margin-bottom: 30px; overflow: hidden; }
            .progress-bar { height: 100%; background-color: #059669; }
        """

    def _normalizar_dados(self, sessao_dados):
        dados_norm = []
        if not sessao_dados:
            return []

        for item in sessao_dados:
            if isinstance(item, dict):
                # Se já for dict, usa direto
                dados_norm.append(item)
            elif isinstance(item, (list, tuple)):
                # CORREÇÃO: Mapeamento baseado no novo LogManager
                # [Nome, Tempo, Status, Linhas, Data, Link]
                nome = str(item[0]) if len(item) > 0 else "Desconhecido"
                tempo = str(item[1]) if len(item) > 1 else "-"
                status = str(item[2]) if len(item) > 2 else "-"
                linhas = str(item[3]) if len(item) > 3 else "-"
                # item[4] é Data (não usado na tabela visual)
                link = str(item[5]) if len(item) > 5 else "#"

                dados_norm.append(
                    {
                        "nome": nome,
                        "tempo": tempo,
                        "status": status,
                        "linhas": linhas,
                        "link": link,
                    }
                )
        return dados_norm

    def gerar_relatorio(self, sessao_dados, tempo_total, pasta_output):
        dados = self._normalizar_dados(sessao_dados)

        total = len(dados)
        if total == 0:
            return None  # Não gera relatório vazio

        # 2. Lógica de Contagem
        # Status possíveis: "Concluído", "Sucesso", "Erro", "Vazio", "Pulado", "Timeout", "Falha"

        sucessos = sum(
            1 for i in dados if "Concluído" in i["status"] or "Sucesso" in i["status"]
        )

        # Considera como erro qualquer coisa que tenha Erro, Falha ou Timeout
        erros = sum(
            1
            for i in dados
            if any(x in i["status"] for x in ["Erro", "Falha", "Timeout"])
        )

        # Vazios ou Pulados são alertas (warning)
        vazios = sum(
            1 for i in dados if "Vazio" in i["status"] or "Pulado" in i["status"]
        )

        # Cálculo de porcentagem de sucesso para barra de progresso
        perc_sucesso = int((sucessos / total) * 100) if total > 0 else 0

        # 3. Gera o HTML
        html = f"""
        <!DOCTYPE html>
        <html lang="pt-BR">
        <head>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <title>Relatório de Auditoria - {datetime.now().strftime('%d/%m/%Y')}</title>
            <style>{self.css}</style>
        </head>
        <body>
            <div class="container">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px;">
                    <h1 style="border: none; margin: 0;">📊 Dashboard de Execução</h1>
                    <span style="color: #6b7280; font-size: 0.9em;">Audit Robot V11</span>
                </div>
                
                <div class="progress-container">
                    <div class="progress-bar" style="width: {perc_sucesso}%;"></div>
                </div>
                
                <div class="summary">
                    <div class="card"><h3>Total Processado</h3><p>{total}</p></div>
                    <div class="card"><h3>Sucessos</h3><p class="success">{sucessos}</p></div>
                    <div class="card"><h3>Vazios/Pulados</h3><p class="empty">{vazios}</p></div>
                    <div class="card"><h3>Erros/Falhas</h3><p class="error">{erros}</p></div>
                </div>

                <div class="info-bar">
                    <span><strong>⏱️ Tempo Total:</strong> {tempo_total}</span>
                    <span><strong>📅 Data da Execução:</strong> {datetime.now().strftime('%d/%m/%Y às %H:%M')}</span>
                </div>

                <table>
                    <thead>
                        <tr>
                            <th style="width: 35%;">Nome do Arquivo</th>
                            <th style="width: 10%;">Tempo</th>
                            <th style="width: 15%;">Status</th>
                            <th style="width: 10%; text-align: center;">Linhas</th>
                            <th style="width: 30%;">Link Fonte</th>
                        </tr>
                    </thead>
                    <tbody>
        """

        for item in dados:
            st = item["status"]
            tempo_item = item.get("tempo", "-")  # Pega o tempo

            # Definição de Classes CSS baseada no status
            if "Concluído" in st or "Sucesso" in st:
                status_cls = "tag-ok"
            elif any(x in st for x in ["Erro", "Falha", "Timeout"]):
                status_cls = "tag-err"
            elif "Pulado" in st or "Vazio" in st:
                status_cls = "tag-warn"
            else:
                status_cls = "tag-info"

            # Truncar link visualmente para não quebrar layout
            link_full = item["link"]
            link_display = link_full
            if len(link_display) > 60:
                link_display = link_display[:60] + "..."

            html += f"""
                <tr>
                    <td><strong>{item['nome']}</strong></td>
                    <td style="font-size: 0.85em; color: #666;">{tempo_item}</td>
                    <td><span class="tag {status_cls}">{st}</span></td>
                    <td style="text-align: center; font-family: 'Consolas', monospace;">{item['linhas']}</td>
                    <td><a href="{link_full}" target="_blank" title="{link_full}">🔗 {link_display}</a></td>
                </tr>
            """

        html += """
                    </tbody>
                </table>
                <div class="footer">
                    Relatório gerado automaticamente pelo Sistema de Auditoria Automatizada.
                    <br>Confidencial - Uso Interno.
                </div>
            </div>
        </body>
        </html>
        """

        # 4. Salva o Arquivo
        filename = f"Relatorio_Execucao_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"

        # Garante que pasta_output é válida
        if not pasta_output or not os.path.exists(pasta_output):
            pasta_output = os.getcwd()

        full_path = os.path.abspath(os.path.join(pasta_output, filename))

        try:
            with open(full_path, "w", encoding="utf-8") as f:
                f.write(html)
            print(f"Relatório HTML salvo em: {full_path}")
            return full_path
        except Exception as e:
            print(f"Erro ao salvar relatório HTML: {e}")
            return None
