"""
Configuración de logging para el pipeline ETL.
Simplificado para uso con AWS Lambda.
"""

import logging
import sys
from pathlib import Path


def setup_logging(
    log_file: Path | str | None = None,
    log_level: str = "INFO",
) -> logging.Logger:
    """
    Configura el sistema de logging para el pipeline.

    Args:
        log_file: Ruta opcional para guardar logs en archivo
        log_level: Nivel de logging (DEBUG, INFO, WARNING, ERROR, CRITICAL)

    Returns:
        Logger configurado
    """
    # Convertir nivel de string a constante
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)

    # Configurar formato
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    # Handlers
    handlers = [logging.StreamHandler(sys.stdout)]

    # Agregar file handler si se especifica
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_path, encoding="utf-8"))

    # Configurar logging básico
    logging.basicConfig(
        level=numeric_level,
        format=log_format,
        datefmt=date_format,
        handlers=handlers,
        force=True,
    )

    logger = logging.getLogger("tecno_etl")
    logger.setLevel(numeric_level)

    return logger


def get_logger(name: str) -> logging.Logger:
    """
    Obtiene un logger con el nombre especificado.

    Args:
        name: Nombre del logger (usualmente __name__)

    Returns:
        Logger configurado
    """
    return logging.getLogger(name)
