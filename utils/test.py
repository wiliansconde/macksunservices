import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

class PoemasDataPlotter:
    def __init__(self, file_path):
        self.file_path = file_path

    def plot_temperature_vs_time(self):
        # Leitura do arquivo de texto para um DataFrame
        df = pd.read_csv(self.file_path, delimiter="|", skiprows=38, names=[
            'UTC_TIME_HOUR', 'UTC_TIME_MINUTE', 'UTC_TIME_SECOND', 'UTC_TIME_MILLISECOND', 'TBMAX', 'TBMIN',
            'NFREQ', 'ELE', 'AZI', 'TBL45', 'TBR45', 'TBL90', 'TBR90', 'FILEPATH_IDX'
        ], low_memory=False)

        # Convertendo as colunas necessárias para o tipo numérico
        df['UTC_TIME_MINUTE'] = pd.to_numeric(df['UTC_TIME_MINUTE'], errors='coerce')
        df['TBL45'] = pd.to_numeric(df['TBL45'], errors='coerce')
        df['TBR45'] = pd.to_numeric(df['TBR45'], errors='coerce')
        df['TBL90'] = pd.to_numeric(df['TBL90'], errors='coerce')
        df['TBR90'] = pd.to_numeric(df['TBR90'], errors='coerce')

        # Remover linhas com valores NaN na coluna UTC_TIME_MINUTE
        df = df.dropna(subset=['UTC_TIME_MINUTE'])

        # Configurando o estilo do seaborn
        sns.set(style="whitegrid")

        # Criando o gráfico de linhas com seaborn
        plt.figure(figsize=(10, 6))
        sns.lineplot(x=df['UTC_TIME_MINUTE'], y=df['TBL45'], label='TBL45', color='blue')
        sns.lineplot(x=df['UTC_TIME_MINUTE'], y=df['TBR45'], label='TBR45', color='orange')
        sns.lineplot(x=df['UTC_TIME_MINUTE'], y=df['TBL90'], label='TBL90', color='green')
        sns.lineplot(x=df['UTC_TIME_MINUTE'], y=df['TBR90'], label='TBR90', color='red')

        plt.xlabel('UTC_TIME_MINUTE')
        plt.ylabel('Temperatura (Kelvin)')
        plt.title('Evolução da Temperatura vs UTC_TIME_MINUTE (Gráfico de Linhas com Seaborn)')
        plt.legend()
        plt.show()

