"""
Modelos Pydantic para validación de esquemas de datos.

Este módulo define los esquemas de validación para todas las entidades
del pipeline ETL, asegurando calidad de datos desde la ingesta.
"""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator


class CategoryRecord(BaseModel):
    """Modelo de validación para registros de la tabla de categorías."""

    codigo_producto: str = Field(
        ..., min_length=1, max_length=50, description="Código único del producto"
    )
    nombre_del_producto: str = Field(
        ..., min_length=1, max_length=500, description="Nombre descriptivo del producto"
    )
    categoria: str = Field(..., min_length=1, max_length=100, description="Categoría del producto")

    @field_validator("codigo_producto")
    @classmethod
    def validate_codigo_producto(cls, v: str) -> str:
        """Valida que el código de producto no esté vacío ni sea solo espacios."""
        v = v.strip().upper()
        if not v:
            raise ValueError("El código de producto no puede estar vacío")
        return v

    @field_validator("nombre_del_producto", "categoria")
    @classmethod
    def validate_text_fields(cls, v: str) -> str:
        """Valida y normaliza campos de texto."""
        v = v.strip().upper()
        if not v:
            raise ValueError("El campo no puede estar vacío")
        return v

    model_config = {
        "str_strip_whitespace": True,
        "validate_assignment": True,
    }


class SalesRecord(BaseModel):
    """Modelo de validación para registros de ventas."""

    codigo_producto: str = Field(
        ..., min_length=1, max_length=50, description="Código del producto vendido"
    )
    cantidad: int | float = Field(..., gt=0, description="Cantidad vendida (debe ser mayor a 0)")
    precio_unitario: float = Field(..., gt=0, description="Precio unitario (debe ser mayor a 0)")
    fecha: datetime | str = Field(..., description="Fecha de la venta")

    # Campos opcionales comunes
    comprobante_num: str | None = Field(default=None, description="Número de comprobante")
    cliente: str | None = Field(default=None, description="Nombre del cliente")

    @field_validator("codigo_producto")
    @classmethod
    def validate_codigo_producto(cls, v: str) -> str:
        """Normaliza el código de producto."""
        v = v.strip().upper()
        if not v:
            raise ValueError("El código de producto no puede estar vacío")
        return v

    @field_validator("cantidad")
    @classmethod
    def validate_cantidad(cls, v: int | float) -> float:
        """Valida que la cantidad sea positiva."""
        if v <= 0:
            raise ValueError(f"La cantidad debe ser mayor a 0, recibido: {v}")
        return float(v)

    @field_validator("precio_unitario")
    @classmethod
    def validate_precio(cls, v: float) -> float:
        """Valida que el precio sea positivo y razonable."""
        if v <= 0:
            raise ValueError(f"El precio debe ser mayor a 0, recibido: {v}")
        if v > 1_000_000:
            raise ValueError(
                f"Precio sospechosamente alto (>{1_000_000:,}), "
                f"recibido: {v:,.2f}. Verifica si es correcto."
            )
        return round(v, 2)

    @field_validator("fecha")
    @classmethod
    def validate_fecha(cls, v: datetime | str) -> datetime:
        """Valida y convierte la fecha."""
        if isinstance(v, str):
            try:
                # Intenta parsear diferentes formatos comunes
                for fmt in ["%Y-%m-%d", "%d/%m/%Y", "%Y-%m-%d %H:%M:%S"]:
                    try:
                        v = datetime.strptime(v, fmt)
                        break
                    except ValueError:
                        continue
                else:
                    raise ValueError(f"Formato de fecha no reconocido: {v}")
            except Exception as e:
                raise ValueError(f"Error al parsear fecha '{v}': {e}")

        # Validar que la fecha no sea futura
        if v > datetime.now():
            raise ValueError(f"La fecha de venta no puede ser futura: {v.strftime('%Y-%m-%d')}")

        # Validar que la fecha no sea muy antigua (ej. antes del año 2000)
        if v.year < 2000:
            raise ValueError(f"Fecha sospechosamente antigua: {v.strftime('%Y-%m-%d')}")

        return v

    @model_validator(mode="after")
    def validate_total(self) -> "SalesRecord":
        """Valida que el total de la venta sea razonable."""
        total = self.cantidad * self.precio_unitario
        if total > 10_000_000:
            raise ValueError(
                f"Total de venta sospechosamente alto: {total:,.2f}. Verifica cantidad y precio."
            )
        return self

    model_config = {
        "str_strip_whitespace": True,
        "validate_assignment": True,
    }


class StockRecord(BaseModel):
    """Modelo de validación para registros de stock."""

    codigo_producto: str = Field(
        ..., min_length=1, max_length=50, description="Código del producto"
    )
    stock_disponible: int | float = Field(
        ..., ge=0, description="Stock disponible (no puede ser negativo)"
    )
    fecha_actualizacion: datetime | str | None = Field(
        default=None, description="Fecha de actualización del stock"
    )

    @field_validator("codigo_producto")
    @classmethod
    def validate_codigo_producto(cls, v: str) -> str:
        """Normaliza el código de producto."""
        v = v.strip().upper()
        if not v:
            raise ValueError("El código de producto no puede estar vacío")
        return v

    @field_validator("stock_disponible")
    @classmethod
    def validate_stock(cls, v: int | float) -> int:
        """Valida que el stock no sea negativo."""
        if v < 0:
            raise ValueError(f"El stock no puede ser negativo, recibido: {v}")
        return int(v)

    @field_validator("fecha_actualizacion")
    @classmethod
    def validate_fecha(cls, v: datetime | str | None) -> datetime | None:
        """Valida la fecha de actualización si está presente."""
        if v is None:
            return None

        if isinstance(v, str):
            try:
                for fmt in ["%Y-%m-%d", "%d/%m/%Y", "%Y-%m-%d %H:%M:%S"]:
                    try:
                        v = datetime.strptime(v, fmt)
                        break
                    except ValueError:
                        continue
                else:
                    raise ValueError(f"Formato de fecha no reconocido: {v}")
            except Exception as e:
                raise ValueError(f"Error al parsear fecha '{v}': {e}")

        return v

    model_config = {
        "str_strip_whitespace": True,
        "validate_assignment": True,
    }


class ValidationError(BaseModel):
    """Modelo para registrar errores de validación."""

    row_number: int = Field(..., description="Número de fila con error")
    field_name: str | None = Field(None, description="Campo que falló la validación")
    error_message: str = Field(..., description="Mensaje de error")
    raw_value: Any = Field(None, description="Valor que causó el error")
    timestamp: datetime = Field(default_factory=datetime.now, description="Timestamp del error")

    model_config = {
        "arbitrary_types_allowed": True,
    }
