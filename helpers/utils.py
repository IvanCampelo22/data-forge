import unicodedata
import re


def normalize_string(value) -> str:
    """
    Converte strings para minúsculas, substitui espaços por '_', remove acentos e caracteres especiais.

    Args:
        value (str): String de entrada a ser normalizada.

    Returns:
        str: String normalizada.
    """
    if value is None:
        return ""

    if not isinstance(value, str):
        value = str(value)

    value = unicodedata.normalize('NFKD', value).encode('ASCII', 'ignore').decode('utf-8')
    value = re.sub(r'[^a-zA-Z0-9\s_]', '', value)
    value = value.strip().replace(" ", "_")
    value = re.sub(r'_+', '_', value)

    return value.lower()
