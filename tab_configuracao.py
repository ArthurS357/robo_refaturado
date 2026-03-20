import tkinter as tk
from tkinter import ttk, messagebox
import time

from tab_base import BaseTab


class TabConfiguracao(BaseTab):
    def montar(self):
        container = self._criar_area_rolavel(self.parent)
        content_frame = ttk.Frame(container, padding=20)
        content_frame.pack(fill="both", expand=True)

        # --- 1. Conexão Browser ---
        fr_chr = ttk.LabelFrame(
            content_frame,
            text=" Conexão Browser ",
            style="Card.TLabelframe",
            padding=15,
        )
        fr_chr.pack(fill="x", pady=5)
        ttk.Label(
            fr_chr,
            text="1. Feche todos os Chromes.\n2. Clique abaixo para abrir a porta de Debug (9222).",
            style="Card.TLabel",
        ).pack(anchor="w")
        ttk.Button(
            fr_chr, text="Abrir Chrome em Modo Debug", command=self.abrir_debug
        ).pack(fill="x", pady=5)

        # --- 2. Espera Inteligente ---
        fr_adv = ttk.LabelFrame(
            content_frame,
            text=" Espera Inteligente ",
            style="Card.TLabelframe",
            padding=15,
        )
        fr_adv.pack(fill="x", pady=10)
        ttk.Label(
            fr_adv,
            text="XPath do Elemento para Aguardar (Opcional):",
            style="Card.TLabel",
        ).pack(anchor="w")
        ttk.Entry(
            fr_adv, textvariable=self.app.xpath_wait, font=self.app.font_mono
        ).pack(fill="x", pady=5)

        # --- 3. Calibração ---
        fr_cal = ttk.LabelFrame(
            content_frame,
            text=" Calibração de Cliques Físicos ",
            style="Card.TLabelframe",
            padding=15,
        )
        fr_cal.pack(fill="x", pady=10)
        fr_cal.columnconfigure(1, weight=1)

        for i, (nome, chave) in enumerate(self.app.botoes_nomes):
            ttk.Label(fr_cal, text=nome, style="Card.TLabel").grid(
                row=i, column=0, sticky="w", pady=2, padx=(0, 10)
            )
            ttk.Entry(fr_cal, textvariable=self.app.coords[chave], width=15).grid(
                row=i, column=1, sticky="w", pady=2
            )

        next_row = len(self.app.botoes_nomes)
        ttk.Button(
            fr_cal,
            text="Iniciar Wizard de Calibração (Automático)",
            style="Primary.TButton",
            command=self.iniciar_calibracao_wizard,
        ).grid(row=next_row, column=0, columnspan=2, sticky="ew", pady=(10, 0))

        # --- 4. Geral ---
        fr_gen = ttk.LabelFrame(
            content_frame, text=" Geral ", style="Card.TLabelframe", padding=15
        )
        fr_gen.pack(fill="x", pady=5)

        f_dl = ttk.Frame(fr_gen, style="Card.TFrame")
        f_dl.pack(fill="x")
        f_dl.columnconfigure(1, weight=1)

        ttk.Label(f_dl, text="Timeout Download (s):", style="Card.TLabel").grid(
            row=0, column=0, sticky="w"
        )
        ttk.Scale(f_dl, from_=60, to=600, variable=self.app.tempo_persistencia).grid(
            row=0, column=1, sticky="ew", padx=10
        )
        ttk.Label(
            f_dl, textvariable=self.app.tempo_persistencia, style="Card.TLabel"
        ).grid(row=0, column=2, sticky="w")

        ttk.Label(f_dl, text="Persistência Botão (s):", style="Card.TLabel").grid(
            row=1, column=0, sticky="w", pady=5
        )
        ttk.Spinbox(
            f_dl, from_=10, to=300, textvariable=self.app.tempo_persist_btn, width=5
        ).grid(row=1, column=1, sticky="w", padx=10)

        ttk.Checkbutton(
            fr_gen,
            text="Limpar arquivos temporários (.tmp) após mover",
            variable=self.app.limpar_apos_mover,
            style="TCheckbutton",
        ).pack(anchor="w", pady=5)

        ttk.Button(
            content_frame,
            text="Salvar Configurações",
            style="Primary.TButton",
            command=self.salvar_cfg,
        ).pack(fill="x", pady=20)

    # ==========================
    # 2. LÓGICA DE CONFIGURAÇÃO
    # ==========================
    def abrir_debug(self):
        _, msg = self.app.motor.abrir_navegador_debug()
        self.app.log(msg)

    def salvar_cfg_interno(self):
        """Salva as variáveis silenciosamente no JSON via AuditMotor"""
        d = {
            "path": self.app.path_rede.get(),
            "dark_mode": self.app.dark_mode.get(),
            "timeout_dl": self.app.tempo_persistencia.get(),
            "timeout_retry_btn": self.app.tempo_persist_btn.get(),
            "clean_after": self.app.limpar_apos_mover.get(),
            "xpath_wait": self.app.xpath_wait.get(),
            "coords": {k: v.get() for k, v in self.app.coords.items()},
        }
        self.app.motor.salvar_config(d)

    def salvar_cfg(self):
        """Salva as variáveis e aplica visualmente"""
        self.salvar_cfg_interno()
        self.app.aplicar_tema()
        messagebox.showinfo("Config", "Configurações salvas.")

    def iniciar_calibracao_wizard(self):
        if not messagebox.askyesno(
            "Calibrar",
            "O Chrome está visível?\n\nVocê terá 3 segundos para posicionar o rato sobre cada botão solicitado.",
        ):
            return

        top = tk.Toplevel(self.app)
        top.geometry("400x150")
        top.attributes("-topmost", True)
        lbl = tk.Label(top, text="Iniciando...", font=("Segoe UI", 12), wraplength=380)
        lbl.pack(expand=True, fill="both", padx=20, pady=20)
        top.focus_force()

        def _step(idx):
            if idx >= len(self.app.botoes_nomes):
                top.destroy()
                self.salvar_cfg()
                return

            nome, key = self.app.botoes_nomes[idx]
            for i in range(3, 0, -1):
                lbl.config(
                    text=f"Posicione o rato sobre:\n\n👉 {nome}\n\nCapturando em {i}s..."
                )
                top.update()
                time.sleep(1)

            x, y = self.app.motor.pegar_mouse_pos()
            self.app.coords[key].set(f"{x},{y}")
            lbl.config(text=f"Capturado! {x},{y}")
            top.update()
            self.app.motor.clique_fisico(x, y)
            self.salvar_cfg_interno()
            time.sleep(0.5)
            self.app.after(100, lambda: _step(idx + 1))

        self.app.after(500, lambda: _step(0))
