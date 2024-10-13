import abc # cria a estrutura base para ETL
import logging
import typing
from pathlib import Path

import pandas as pd


class BaseETL(abc.ABC):
    """
    Está classe é o esqueleto para implementar as demais classes do
    ETL.\n
    Neste objeto deverão ser informados os caminhos para as pastas
    de entrada e de saída de dados, além das uma flag informando se
    nós devemos re-processar os dados.
    """
    caminho_entrada: Path
    caminho_saida: Path
    reprocessar: bool
    _dados_entrada: typing.Dict[str, pd.DataFrame]
    _dados_saida: typing.Dict[str, pd.DataFrame]
    logger: logging.Logger

    def __init__(
            self,
            entrada: typing.Union[str, Path],
            saida: typing.Union[str, Path],
            criar_caminho: bool = True,
            reprocessar: bool = True,
    ) -> None:
        """
        Instância o objeto de Base ETL.
        :param entrada: string com caminho para pasta de entrada
        :param saida: string com caminho para pasta de saída
        :param criar_caminho: flag indicando se devemos criar os caminhos
        :param reprocessar: flag para forçar o re-processamento das bases de dados
        """
        self.caminho_entrada = Path(entrada)
        self.caminho_saida = Path(saida)
        self._dados_entrada = None
        self._dados_saida = None
        self.reprocessar = reprocessar
        self.criar_caminho = criar_caminho

        if criar_caminho:
            self.caminho_entrada.mkdir(parents=True, exist_ok=True)
            self.caminho_saida.mkdir(parents=True, exist_ok=True)

        self._dados_entrada = dict()
        self._dados_saida = dict()
        self.logger = logging.getLogger(__name__)

    def __str__(self) -> str:
        """
        Representação da classe em modo texto
        """
        return self.__class__.__name__

    @abc.abstractmethod
    def extract(self) -> None:
        """
        Extraí os dados do objeto.
        """
        pass

    @abc.abstractmethod
    def transform(self) -> None:
        """
        Transforma os dados e os adequa para os formatos de
        saída de interesse.
        """
        pass

    def load(self) -> None:
        """
        Exporta os dados transformados.
        """
        # for arq, df in self.dados_saida.items():
        #     df.to_parquet(self.caminho_saida / arq, index=False)
        pass

    def pipeline(self) -> None:
        """
        Executa o pipeline completo de tratamento de dados.
        """
        self.extract()
        self.transform()
        self.load()