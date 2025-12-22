"""
PROJETO ETL - CIÊNCIA DE DADOS de placas de vídeo NVIDIA
Pipeline completo para processamento de dados de placas de vídeo NVIDIA
"""

import json
from datetime import datetime
import logging
import sys
import os

# ====================== CONFIGURAÇÃO =======================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('etl_pipeline.log', encoding='utf-8', mode='a')
    ]
)
logger = logging.getLogger(__name__)

class PipelineETLGPU:
    """Pipeline ETL para análise de placas de vídeo NVIDIA"""

    def __init__(self):
        self.metrics = {}
        self.gpus = []

    # ====================== EXTRACT =======================
    def extract(self):
        logger.info("EXTRACT: Coletando dados de GPUs NVIDIA")

        self.gpus = [
            {"id": 1, "modelo": "RTX 3060", "vram_gb": 12, "preco": 1700, "benchmark": 100, "ano": 2021},
            {"id": 2, "modelo": "RTX 3070", "vram_gb": 8, "preco": 2300, "benchmark": 130, "ano": 2020},
            {"id": 3, "modelo": "RTX 4060", "vram_gb": 8, "preco": 1900, "benchmark": 125, "ano": 2023},
            {"id": 4, "modelo": "RTX 4090", "vram_gb": 24, "preco": 9000, "benchmark": 250, "ano": 2022},
            {"id": 5, "modelo": "GTX 1660 Super", "vram_gb": 6, "preco": 900, "benchmark": 70, "ano": 2019},
            {"id": 6, "modelo": "RTX 2060", "vram_gb": 6, "preco": 1100, "benchmark": 85, "ano": 2019}
        ]

        logger.info(f"{len(self.gpus)} GPUs extraídas")
        return self.gpus

    # ====================== TRANSFORM =======================
    def transform(self):
        logger.info("TRANSFORM: Processando GPUs")

        timestamp = datetime.now().isoformat()

        for gpu in self.gpus:
            gpu["categoria"] = self._categorize_gpu(gpu)
            gpu["mensagem_ia"] = self._generate_ai_message(gpu)
            gpu["recomendacoes"] = self._generate_recommendations(gpu)
            gpu["alertas"] = self._generate_alerts(gpu)
            gpu["processado_em"] = timestamp
            gpu["versao_pipeline"] = "1.0"

        self.metrics = self._calculate_metrics(self.gpus)
        return self.gpus

    # ====================== REGRAS =======================
    def _categorize_gpu(self, gpu):
        score = gpu["benchmark"]

        if score < 80:
            return "Entrada"
        elif score < 120:
            return "Intermediária"
        elif score < 180:
            return "Avançada"
        else:
            return "Enthusiast"

    def _generate_ai_message(self, gpu):
        modelo = gpu["modelo"]
        preco = gpu["preco"]
        benchmark = gpu["benchmark"]

        custo_beneficio = benchmark / preco

        if custo_beneficio > 0.07:
            return f"🔥 {modelo} tem excelente custo-benefício."
        elif custo_beneficio > 0.05:
            return f"✅ {modelo} oferece bom equilíbrio entre preço e desempenho."
        else:
            return f"⚠️ {modelo} entrega alto desempenho, mas com custo elevado."

    def _generate_recommendations(self, gpu):
        recs = []

        if gpu["vram_gb"] < 8:
            recs.append("Pode sofrer limitações em jogos modernos")

        if gpu["ano"] < 2020:
            recs.append("Modelo antigo — considere upgrade")

        if gpu["benchmark"] > 150:
            recs.append("Ideal para 4K, Ray Tracing e IA")

        if not recs:
            recs.append("Ótima opção para 1080p/1440p")

        return recs

    def _generate_alerts(self, gpu):
        if gpu["vram_gb"] <= 6:
            return {"nivel": "ALTO", "mensagem": "VRAM limitada"}
        elif gpu["preco"] > 7000:
            return {"nivel": "MÉDIO", "mensagem": "Preço muito elevado"}
        else:
            return {"nivel": "BAIXO", "mensagem": "Sem alertas"}

    # ====================== MÉTRICAS =======================
    def _calculate_metrics(self, gpus):
        benchmarks = [g["benchmark"] for g in gpus]
        precos = [g["preco"] for g in gpus]

        return {
            "total_gpus": len(gpus),
            "preco_medio": sum(precos) / len(precos),
            "benchmark_medio": sum(benchmarks) / len(benchmarks),
            "benchmark_max": max(benchmarks),
            "benchmark_min": min(benchmarks),
            "categorias": self._count_categories(gpus)
        }

    def _count_categories(self, gpus):
        cat = {}
        for g in gpus:
            c = g["categoria"]
            cat[c] = cat.get(c, 0) + 1
        return cat

    # ====================== LOAD =======================
    def load(self):
        logger.info("LOAD: Salvando resultados")

        os.makedirs("output", exist_ok=True)

        with open("output/gpus_nvidia_processadas.json", "w", encoding="utf-8") as f:
            json.dump({
                "metadata": {
                    "processado_em": datetime.now().isoformat(),
                    "pipeline": "ETL GPUs NVIDIA"
                },
                "metricas": self.metrics,
                "gpus": self.gpus
            }, f, indent=2, ensure_ascii=False)

        with open("output/relatorio_gpus_nvidia.md", "w", encoding="utf-8") as f:
            f.write("# Relatório de GPUs NVIDIA\n\n")
            f.write(f"Total de GPUs: {self.metrics['total_gpus']}\n")
            f.write(f"Preço médio: R$ {self.metrics['preco_medio']:.2f}\n")
            f.write(f"Benchmark médio: {self.metrics['benchmark_medio']:.1f}\n\n")

        return True

    # ====================== RUN =======================
    def run(self):
        logger.info("🚀 Iniciando pipeline ETL GPUs NVIDIA")

        if not self.extract():
            return False
        self.transform()
        self.load()

        logger.info("✅ Pipeline finalizado com sucesso")
        return True


# ====================== EXECUÇÃO ======================
if __name__ == "__main__":
    pipeline = PipelineETLGPU()
    sys.exit(0 if pipeline.run() else 1)