from fastapi import APIRouter, HTTPException, Query, status, BackgroundTasks
from fastapi.responses import FileResponse, JSONResponse
import pandas as pd
import psycopg2
from api.v1.apps.clipping.service.clipping_service import save_news_service
from psycopg2.extras import RealDictCursor
from datetime import datetime
import os
from config.config import DB_HOST_CLIPPING, DB_NAME_CLIPPING, DB_OPTIONS_CLIPPING, DB_PASSWORD_CLIPPING, DB_USER_CLIPPING
from config.config import DB_HOST_HANDSON, DB_NAME_HANDSON, DB_PASSWORD_HANDSON, DB_USER_HANDSON
from fastapi.encoders import jsonable_encoder
from loguru import logger
from typing import List, Union, Dict
import unicodedata
import re
from psycopg2 import sql

router = APIRouter()

def parse_date(date_str):
    """
    Tenta converter uma string de data para um formato 'YYYY-MM-DD',
    suportando diferentes formatos de entrada.
    """
    formats = ["%d/%m/%Y %H:%M:%S", "%d/%m/%Y"]
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    raise ValueError(f"Formato de data inválido: {date_str}")

# FIXME melhorar a estrutura do código, separar em partes menores
# TODO colocar a lógica principal dentro de uma task do celery e importar nesse endpoint
def normalize_column_name(name: str) -> str:
    """Converte nomes de colunas para minúsculas, substitui espaços por '_', remove acentos e caracteres especiais."""
    name = unicodedata.normalize('NFKD', name).encode('ASCII', 'ignore').decode('utf-8')  # Remove acentos
    name = re.sub(r'[^a-zA-Z0-9_]', '_', name)  # Substitui espaços e caracteres especiais por '_'
    name = re.sub(r'__+', '_', name)  # Remove underscores duplos
    return name.lower()  # Converte para minúsculas

def clean_value(value: Union[str, None], is_integer: bool = False, is_string: bool = False) -> Union[str, int, None]:
    """
    Normaliza os valores:
    - Se for string vazia, retorna None.
    - Se for inteiro e inválido, retorna 0.
    - Se for string e deve ser forçada para VARCHAR, converte para string.
    """
    if value is None or value == "":
        return None if not is_integer else 0

    if is_integer:
        return int(value)

    if is_string:
        return str(value)  # 🔹 Converte explicitamente para string

    return value

@router.post(
    "/upload_file/{table_name}/",
    responses={
        201: {"description": "Dados salvos com sucesso", "content": {"application/json": {}}},
        400: {"description": "Insira dados válidos"},
    },
    status_code=status.HTTP_201_CREATED,
)
def save_news(json_data: List[dict], company_id_clipping: str, schema_name: str, table_name: str = None):
    """Upload de notícias para a tabela de news no clipping e para o hands-on."""
    connection_handson = None
    cursor_handson = None
    connection_clipping = None
    cursor_clipping = None
    print(company_id_clipping)

    try:
        # 🔹 **Conexão com o banco HANDSON**
        connection_handson = psycopg2.connect(
            dbname=DB_NAME_HANDSON,
            user=DB_USER_HANDSON,
            password=DB_PASSWORD_HANDSON,
            host=DB_HOST_HANDSON,
        )
        cursor_handson = connection_handson.cursor()

        # 🔹 **Conexão com o banco CLIPPING**
        connection_clipping = psycopg2.connect(
            dbname=DB_NAME_CLIPPING,
            user=DB_USER_CLIPPING,
            password=DB_PASSWORD_CLIPPING,
            host=DB_HOST_CLIPPING,
            options=DB_OPTIONS_CLIPPING
        )
        cursor_clipping = connection_clipping.cursor()

        results = []

        for index, item in enumerate(json_data, start=1):
            try:
                publication_date = parse_date(item["DATA"])
            except ValueError:
                logger.error(f"Erro ao processar a data no item {index}: {item.get('DATA')}")
                raise HTTPException(
                    status_code=400,
                    detail=f"Erro ao processar a data no item {index}. Formato inválido: {item['DATA']}",
                )

            news_id = clean_value(item["CÓDIGO DA NOTÍCIA"], is_string=True)

            clipping_data = {
                "news_code": news_id,
                "publication_date": publication_date,
                "vehicle": item.get("VEÍCULO"),
                "title": item.get("TÍTULO"),
                "theme": item.get("TEMA"),
                "subject_name_slug": item.get("MICROTEMA"),
                "media_type": item.get("TIPO VEÍCULO"),
                "tier": clean_value(item.get("TIER", 0), is_integer=True),
                "feeling": item.get("SENTIMENTO"),
                "readers": clean_value(item.get("ALCANCE", 0), is_integer=True),
                "journalist": item.get("JORNALISTA"),
                "original_link": item.get("LINK ORIGINAL"),
                "valuation": str(item.get("VALORAÇÃO", "0.00")).replace("R$", "").replace(".", "").replace(",", "."),
                "created_date": datetime.now(),
                "modified_date": datetime.now(),
                "is_active": True,
                "approved_news": True,
                "company_id": clean_value(company_id_clipping, is_integer=True)
            }

            results.append({"table": "clippings_news", "id": news_id})

            cursor_clipping.execute("""
                SELECT news_code FROM news_charisma.clippings_news WHERE news_code = %s;
            """, (news_id,))
            existing_news = cursor_clipping.fetchone()

            if existing_news:
                update_fields = ', '.join([f"{col} = %s" for col in clipping_data.keys()])
                update_values = tuple(clipping_data.values()) + (news_id,)

                update_clipping_query = f"""
                    UPDATE news_charisma.clippings_news
                    SET {update_fields}
                    WHERE news_code = %s;
                """
                cursor_clipping.execute(update_clipping_query, update_values)
            else:
                clipping_columns = ", ".join(clipping_data.keys())
                clipping_placeholders = ", ".join(["%s"] * len(clipping_data))
                clipping_values = tuple(clipping_data.values())

                insert_clipping_query = f"""
                    INSERT INTO news_charisma.clippings_news ({clipping_columns})
                    VALUES ({clipping_placeholders})
                    RETURNING id;
                """
                cursor_clipping.execute(insert_clipping_query, clipping_values)

            connection_clipping.commit()

            cursor_handson.execute(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = %s;
            """, (table_name,))
            table_columns = {row[0] for row in cursor_handson.fetchall()}

            normalized_data = {
                normalize_column_name(key): clean_value(value, is_integer=False)
                for key, value in item.items()
                if normalize_column_name(key) in table_columns
            }

            if "news_code" in table_columns:
                normalized_data["news_code"] = news_id
            
            if "date" in table_columns:
                normalized_data["date"] = publication_date

            if "company_id" in table_columns:
                normalized_data["company_id"] = company_id_clipping
            
            if normalized_data:
                columns = ", ".join(normalized_data.keys())
                placeholders = ", ".join(["%s"] * len(normalized_data))
                values = tuple(normalized_data.values())

                update_fields = ', '.join([f"{col} = EXCLUDED.{col}" for col in normalized_data.keys()])

                upsert_query = f"""
                    INSERT INTO {schema_name}.{table_name} ({columns})
                    VALUES ({placeholders})
                    ON CONFLICT (news_code, company_id, date)
                    DO UPDATE SET {update_fields}
                    RETURNING id;
                """
                cursor_handson.execute(upsert_query, values)
                table_id = cursor_handson.fetchone()[0]
                results.append({"table": table_name, "id": table_id})

        connection_handson.commit()

        return jsonable_encoder({"message": "Dados salvos com sucesso", "data": results})
    
    except Exception as e:
        logger.error(f"Erro interno: {e}")
        if connection_handson:
            connection_handson.rollback()
        if connection_clipping:
            connection_clipping.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Erro interno: {str(e)}"
        )
    finally:
        if cursor_handson:
            cursor_handson.close()
        if connection_handson:
            connection_handson.close()
        if cursor_clipping:
            cursor_clipping.close()
        if connection_clipping:
            connection_clipping.close()




# FIXME melhorar a estrutura do código, separar em partes menores
# TODO colocar a lógica principal dentro de uma task do celery e importar nesse endpoint
@router.get("/export/")
async def export_to_excel(
    table_name: str, 
    schema_name: str,
    start_date: str, 
    end_date: str, 
    background_tasks: BackgroundTasks
):
    """
    Exporta os dados combinados da tabela dinâmica e 'clippings_news' para um arquivo Excel.
    Se não houver dados em 'clippings_news', apenas os dados do 'hands-on' são exportados sem a coluna `news_code`.
    """
    file_path = None
    try:
        logger.info("Estabelecendo conexões com os bancos de dados...")
        handson_connection = psycopg2.connect(
            dbname=DB_NAME_HANDSON,
            user=DB_USER_HANDSON,
            password=DB_PASSWORD_HANDSON,
            host=DB_HOST_HANDSON,
        )
        clipping_connection = psycopg2.connect(
            dbname=DB_NAME_CLIPPING,
            user=DB_USER_CLIPPING,
            password=DB_PASSWORD_CLIPPING,
            host=DB_HOST_CLIPPING,
            options=DB_OPTIONS_CLIPPING
        )

        handson_cursor = handson_connection.cursor(cursor_factory=RealDictCursor)
        clipping_cursor = clipping_connection.cursor(cursor_factory=RealDictCursor)

        logger.info(f"Buscando dados da tabela '{table_name}' entre {start_date} e {end_date}...")
        handson_cursor.execute(f"""
            SELECT *
            FROM {schema_name}.{table_name}
            WHERE is_deleted = FALSE
            AND date BETWEEN %s AND %s;
        """, (start_date, end_date))
        dynamic_data = handson_cursor.fetchall()

        if not dynamic_data:
            logger.warning(f"Nenhum dado encontrado na tabela {table_name} no intervalo especificado.")
            raise HTTPException(status_code=404, detail=f"Nenhum dado encontrado no intervalo de datas especificado.")

        logger.info("Filtrando `news_code` válidos...")
        news_codes = [
            str(row["news_code"]) for row in dynamic_data 
            if "news_code" in row and row["news_code"]
        ]

        logger.info(f"Total de news_codes válidos: {len(news_codes)}")

        if news_codes:
            logger.info(f"Buscando dados da tabela 'clippings_news' para os news_codes: {news_codes}")
            placeholder = ",".join(["%s"] * len(news_codes))
            clipping_cursor.execute(f"""
                SELECT *
                FROM clippings_news
                WHERE news_code IN ({placeholder});
            """, tuple(news_codes))
            clipping_data = clipping_cursor.fetchall()
        else:
            clipping_data = []

        logger.info("Convertendo dados para DataFrames...")
        df_dynamic = pd.DataFrame(dynamic_data)

        if clipping_data:
            logger.info("Dados encontrados em 'clippings_news', combinando com a tabela dinâmica...")
            df_clippings = pd.DataFrame(clipping_data)

            logger.info("Removendo colunas desnecessárias da tabela dinâmica...")
            df_dynamic = df_dynamic.drop(columns=["id", "news_code"], errors="ignore")

            logger.info("Combinando os dados lado a lado...")
            df_combined = pd.concat([df_clippings.reset_index(drop=True), df_dynamic.reset_index(drop=True)], axis=1)
        else:
            logger.warning("Nenhum dado encontrado em 'clippings_news'. Exportando apenas os dados do hands-on.")
            df_combined = df_dynamic.drop(columns=["news_code"], errors="ignore")  # 🚀 REMOVE `news_code` QUANDO NÃO HÁ `clippings_news`

        logger.info("Removendo timezone dos campos datetime...")
        for col in df_combined.select_dtypes(include=["datetime64[ns, UTC]"]).columns:
            df_combined[col] = df_combined[col].dt.tz_localize(None)

        logger.info("Gerando o arquivo Excel...")
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        file_path = f"/tmp/exported_data_{timestamp}.xlsx"
        df_combined.to_excel(file_path, index=False)

        background_tasks.add_task(os.remove, file_path)

        logger.info(f"Arquivo Excel gerado em: {file_path}")
        return FileResponse(
            file_path,
            filename=f"{table_name}_{start_date}_{end_date}.xlsx",
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    except Exception as e:
        logger.error(f"Erro ao exportar os dados: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro ao exportar os dados: {str(e)}")
    finally:
        if handson_cursor:
            handson_cursor.close()
        if clipping_cursor:
            clipping_cursor.close()
        if handson_connection:
            handson_connection.close()
        if clipping_connection:
            clipping_connection.close()
            

@router.get("/get_data/{schema_name}/{table_name}/")
async def get_combined_data(
    schema_name: str,
    table_name: str,
    limit: int = Query(10, description="Número máximo de registros por página", ge=1),
    offset: int = Query(0, description="Número de registros a serem ignorados antes de retornar os resultados", ge=0)
):
    """
    Obtém dados de uma tabela dinâmica e combina com dados de clippings_news, com suporte a paginação.
    """
    clipping_connection = None
    dynamic_connection = None

    try:
        clipping_connection = psycopg2.connect(
            dbname=DB_NAME_CLIPPING,
            user=DB_USER_CLIPPING,
            password=DB_PASSWORD_CLIPPING,
            host=DB_HOST_CLIPPING,
            port="5432",
            options=DB_OPTIONS_CLIPPING,
        )

        dynamic_connection = psycopg2.connect(
            dbname=DB_NAME_HANDSON,
            user=DB_USER_HANDSON,
            password=DB_PASSWORD_HANDSON,
            host=DB_HOST_HANDSON,
        )

        clipping_cursor = clipping_connection.cursor(cursor_factory=RealDictCursor)
        dynamic_cursor = dynamic_connection.cursor(cursor_factory=RealDictCursor)

        dynamic_cursor.execute("""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = %s
                AND table_name = %s
            );
        """, (schema_name, table_name))

        if not dynamic_cursor.fetchone()["exists"]:
            raise HTTPException(status_code=400, detail=f"Tabela {schema_name}.{table_name} não existe.")

        query = sql.SQL("""
            SELECT * FROM {}.{}
            ORDER BY news_code
            LIMIT %s OFFSET %s
        """).format(
            sql.Identifier(schema_name),
            sql.Identifier(table_name)
        )
        dynamic_cursor.execute(query, (limit, offset))
        dynamic_data = dynamic_cursor.fetchall()

        if not dynamic_data:
            return {"message": f"Nenhum dado encontrado na tabela {schema_name}.{table_name}."}

        news_codes = [
            str(row["news_code"]) for row in dynamic_data 
            if "news_code" in row and row["news_code"] is not None
        ]

        if not news_codes:
            return {"message": "Nenhum `news_code` válido encontrado na tabela dinâmica."}

        logger.info(f"news_codes válidos: {news_codes}")

        placeholder = ",".join(["%s"] * len(news_codes))
        clipping_cursor.execute(f"""
            SELECT *
            FROM clippings_news
            WHERE news_code IN ({placeholder});
        """, tuple(news_codes))
        clipping_data = clipping_cursor.fetchall()

        combined_data = []
        for dynamic_row in dynamic_data:
            related_clipping = next(
                (clip for clip in clipping_data if clip["news_code"] == str(dynamic_row["news_code"])), 
                {}

            ) if dynamic_row.get("news_code") is not None else {}

            combined_data.append({
                "dynamic_table_data": dynamic_row,
                "clipping_data": related_clipping
            })

        dynamic_cursor.execute(f"SELECT COUNT(*) FROM {schema_name}.{table_name}")
        total_records = dynamic_cursor.fetchone()["count"]

        return {
            "total_records": total_records,
            "page_size": limit,
            "current_offset": offset,
            "data": combined_data
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao buscar dados: {str(e)}")
    finally:
        if clipping_cursor:
            clipping_cursor.close()
        if dynamic_cursor:
            dynamic_cursor.close()
        if clipping_connection:
            clipping_connection.close()
        if dynamic_connection:
            dynamic_connection.close()


@router.post(
    "/upload-file-handson/{table_name}/",
    responses={
        201: {"description": "Dados salvos com sucesso", "content": {"application/json": {}}},
        400: {"description": "Insira dados válidos"},
    },
    status_code=status.HTTP_201_CREATED,
)
def save_data_hands_on(json_data: List[Dict], schema_name: str, table_name: str, company_id: int):
    """Upload de dados para a tabela no hands-on, sem salvar `news_code`."""
    connection_handson = None
    cursor_handson = None
    
    try:
        connection_handson = psycopg2.connect(
            dbname=DB_NAME_HANDSON,
            user=DB_USER_HANDSON,
            password=DB_PASSWORD_HANDSON,
            host=DB_HOST_HANDSON,
        )
        cursor_handson = connection_handson.cursor()

        results = []

        for index, item in enumerate(json_data, start=1):
            try:
                publication_date = parse_date(item["DATA"])
            except ValueError:
                logger.error(f"Erro ao processar a data no item {index}: {item.get('DATA')}")
                raise HTTPException(
                    status_code=400,
                    detail=f"Erro ao processar a data no item {index}. Formato inválido: {item['DATA']}",
                )

            cursor_handson.execute(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = %s;
            """, (table_name,))
            table_columns = {row[0] for row in cursor_handson.fetchall()}

            normalized_data = {
                normalize_column_name(key): clean_value(value, is_integer=("integer" in table_columns))
                for key, value in item.items()
                if normalize_column_name(key) in table_columns and normalize_column_name(key) != "news_code"
            }

            if "date" in table_columns:
                normalized_data["date"] = publication_date

            normalized_data["company_id"] = company_id

            if normalized_data:
                columns = ", ".join(normalized_data.keys())
                placeholders = ", ".join(["%s"] * len(normalized_data))
                values = tuple(normalized_data.values())

                update_fields = ', '.join([
                    f"{col} = EXCLUDED.{col}" for col in normalized_data.keys()
                ])

                upsert_query = f"""
                    INSERT INTO {schema_name}.{table_name} ({columns})
                    VALUES ({placeholders})
                    ON CONFLICT DO NOTHING
                    RETURNING id;
                """
                logger.info(f"Executando query para HANDSON: {upsert_query} com valores {values}")
                cursor_handson.execute(upsert_query, values)
                table_id = cursor_handson.fetchone()[0]
                results.append({"table": table_name, "id": table_id})

        connection_handson.commit()
        return jsonable_encoder({"message": "Dados salvos com sucesso", "data": results})

    except Exception as e:
        logger.error(f"Erro interno: {e}")
        if connection_handson:
            connection_handson.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Erro interno: {str(e)}"
        )
    finally:
        if cursor_handson:
            cursor_handson.close()
        if connection_handson:
            connection_handson.close()