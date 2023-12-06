-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Manipulacion de Datos
-- MAGIC ------------------------------------
-- MAGIC En este notebook trabajaremos con la columna fecha que no está en un formato estandar, es decir no tiene un número estándar de dígitos para días. Por ejemplo, 12/01/11 tiene un mes de dos dígitos y un día de un dígito. Por lo tanto, para trabajar con una columna fecha el formato debe ser coherente en toda la tabla.
-- MAGIC
-- MAGIC **Para este proposito utilizaremos las siguientes funciones:**
-- MAGIC
-- MAGIC 1. SPLIT: utilizamos la función para dividir la columna invoiceDate.
-- MAGIC
-- MAGIC 2. LPAD: utilizamos la función para rellenar con ceros a la izquierda la columna "día" hasta que tenga una longitud de 2 caracteres.
-- MAGIC
-- MAGIC 3. CONCAT_WS: utilizamos la función para concatenar las columnas "month_", "day_" y "year" con "/" como separador.

-- COMMAND ----------

-- Cargamos nuestro archivo csv y la alojamos en una tabla "df"
DROP TABLE IF EXISTS df;
CREATE TABLE df
USING csv 
OPTIONS (
  path "dbfs:/FileStore/practice/export__1_.csv",
  header "true"
)

-- COMMAND ----------

-- Seleccionamos nuestro df
select*from df limit(2)

-- COMMAND ----------

-- Utilizamos un Common Table Expression (CTE) llamado df_view para transformar los datos de la tabla df, dentro de nuestra tabla df_new

DROP TABLE IF EXISTS df_new;
CREATE TABLE df_new

WITH df_view AS
(
SELECT 
  InvoiceNo,
  StockCode,
  Description,
  Quantity,
  split(invoiceDate, "/")[0] month, -- extraemos el mes de la columna invoiceDate y lo llamamos "month"
  split(invoiceDate, "/")[1] day,   -- extraemos el dia de la columna invoiceDate y lo llamamos "day"
  split(split(invoiceDate, " ")[0], "/")[2] year,   -- extraemos el año de la columna invoiceDate
  LPAD(month, 2, 0) AS month_,   -- rellenamos con ceros a la izquierda de "month" y lo llamamos "month_"
  LPAD(day, 2, 0) AS day_,    -- rellanamos con ceros a la izquierda de "day" y lo llamamos "day_"
  UnitPrice, 
  CustomerID,
  Country
FROM df
)
SELECT 
 InvoiceNo,
  StockCode,
  Description,
  Quantity, 
  concat_ws("/", month_, day_, year) Date_Standar, -- concatenamos las columnas "month_", "day_" y "year"
  UnitPrice,
  CustomerID,
  Country
FROM df_view;

-- COMMAND ----------

-- Seleccionamos nuestra btabla df_new con la fecha modificada y coherente en toda su extension
select*from df_new limit(2)
