![logo_ironhack_blue 7](https://user-images.githubusercontent.com/23629340/40541063-a07a0a8a-601a-11e8-91b5-2f13e4e6b441.png)

# Lab Avanzado | ETL Multi-Tabla con SCD Type 2 y Modelado Dimensional para SEAT

## Objetivo

Implementar un pipeline ETL avanzado que maneje múltiples tablas relacionadas del servicio postventa de SEAT, aplicando SCD Type 2 para dimensiones, modelado dimensional (Star Schema), y técnicas avanzadas de transformación. Este lab requiere conocimientos profundos de ETL, modelado dimensional y técnicas avanzadas de Snowflake.

## Requisitos

* Conocimientos previos: ETL multi-tabla, SCD Type 2, modelado dimensional, stored procedures avanzados.

## Entrega

- Prepara todos los archivos solicitados en la sección "Entregables"
- Sube tu solución al campus en el campo de entrega correspondiente

## 📊 Escenario SEAT

Eres el arquitecto de datos responsable de diseñar un modelo Silver dimensional completo para el servicio postventa. El modelo debe:

- **Mantener historial**: Cambios en vehículos, clientes y talleres deben preservarse
- **Optimizar consultas analíticas**: Estructura dimensional para análisis rápido
- **Integrar múltiples fuentes**: Talleres, vehículos, recambios, satisfacción
- **Soportar análisis complejos**: Por modelo, combustible, región, tiempo

## 🧱 Desafío 1 – Dimensiones con SCD Type 2

### 1.1 Dimensión Vehículo con Historial

Implementa una dimensión de vehículos que capture cambios en propietario, kilómetros, y estado:

```sql
CREATE OR REPLACE TABLE DEV_SILVER_DB.S_VEHICLE.TB_DIM_VEHICLE_SCD2 (
    ID_VEHICLE_SK NUMBER AUTOINCREMENT PRIMARY KEY,
    ID_VEHICLE_BK NUMBER NOT NULL,  -- Business key
    CAT_MODEL VARCHAR(100),
    TYP_FUEL_TYPE VARCHAR(50),
    DTE_REGISTRATION DATE,
    QTY_CURRENT_KM NUMBER,
    ID_CUSTOMER_CURRENT NUMBER,
    CAT_VEHICLE_STATUS VARCHAR(50),  -- 'ACTIVE', 'SOLD', 'SCRAPPED'
    DTE_VALID_FROM TIMESTAMP_NTZ NOT NULL,
    DTE_VALID_TO TIMESTAMP_NTZ,
    FLG_CURRENT BOOLEAN DEFAULT TRUE,
    AUD_TST_CREATE TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    AUD_TST_UPDATE TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (ID_VEHICLE_BK, FLG_CURRENT)
COMMENT = 'Dimensión de vehículos con historial SCD Type 2';
```

### 1.2 Dimensión Cliente con Historial

```sql
CREATE OR REPLACE TABLE DEV_SILVER_DB.S_CUSTOMER.TB_DIM_CUSTOMER_SCD2 (
    ID_CUSTOMER_SK NUMBER AUTOINCREMENT PRIMARY KEY,
    ID_CUSTOMER_BK NUMBER NOT NULL,
    NAM_CUSTOMER VARCHAR(200),
    TYP_CUSTOMER_TYPE VARCHAR(50),  -- 'INDIVIDUAL', 'FLEET', 'DEALER'
    REF_COUNTRY_CODE VARCHAR(3),
    CAT_SEGMENT VARCHAR(50),  -- 'VIP', 'REGULAR', 'NEW'
    DTE_VALID_FROM TIMESTAMP_NTZ NOT NULL,
    DTE_VALID_TO TIMESTAMP_NTZ,
    FLG_CURRENT BOOLEAN DEFAULT TRUE,
    AUD_TST_CREATE TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (ID_CUSTOMER_BK, FLG_CURRENT)
COMMENT = 'Dimensión de clientes con historial SCD Type 2';
```

### 1.3 Stored Procedure para Carga SCD Type 2

```sql
CREATE OR REPLACE PROCEDURE SP_LOAD_VEHICLE_SCD2(
    P_SOURCE_TABLE VARCHAR
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  v_rows_closed NUMBER;
  v_rows_inserted NUMBER;
  v_result VARIANT;
BEGIN
  -- Paso 1: Cerrar registros que han cambiado
  UPDATE DEV_SILVER_DB.S_VEHICLE.TB_DIM_VEHICLE_SCD2 TARGET
  SET 
    DTE_VALID_TO = CURRENT_TIMESTAMP(),
    FLG_CURRENT = FALSE,
    AUD_TST_UPDATE = CURRENT_TIMESTAMP()
  WHERE TARGET.FLG_CURRENT = TRUE
  AND EXISTS (
    SELECT 1
    FROM IDENTIFIER(P_SOURCE_TABLE) SOURCE
    WHERE SOURCE.ID_VEHICLE = TARGET.ID_VEHICLE_BK
    AND (
      SOURCE.CAT_MODEL != TARGET.CAT_MODEL
      OR SOURCE.TYP_FUEL_TYPE != TARGET.TYP_FUEL_TYPE
      OR SOURCE.ID_CUSTOMER_CURRENT != TARGET.ID_CUSTOMER_CURRENT
      OR SOURCE.CAT_VEHICLE_STATUS != TARGET.CAT_VEHICLE_STATUS
      OR ABS(SOURCE.QTY_CURRENT_KM - COALESCE(TARGET.QTY_CURRENT_KM, 0)) > 1000
    )
  );
  
  GET DIAGNOSTICS v_rows_closed = ROW_COUNT;
  
  -- Paso 2: Insertar nuevos registros para cambios
  INSERT INTO DEV_SILVER_DB.S_VEHICLE.TB_DIM_VEHICLE_SCD2 (
    ID_VEHICLE_BK,
    CAT_MODEL,
    TYP_FUEL_TYPE,
    DTE_REGISTRATION,
    QTY_CURRENT_KM,
    ID_CUSTOMER_CURRENT,
    CAT_VEHICLE_STATUS,
    DTE_VALID_FROM,
    FLG_CURRENT
  )
  SELECT DISTINCT
    SOURCE.ID_VEHICLE,
    SOURCE.CAT_MODEL,
    SOURCE.TYP_FUEL_TYPE,
    SOURCE.DTE_REGISTRATION,
    SOURCE.QTY_CURRENT_KM,
    SOURCE.ID_CUSTOMER_CURRENT,
    SOURCE.CAT_VEHICLE_STATUS,
    CURRENT_TIMESTAMP(),
    TRUE
  FROM IDENTIFIER(P_SOURCE_TABLE) SOURCE
  WHERE NOT EXISTS (
    SELECT 1
    FROM DEV_SILVER_DB.S_VEHICLE.TB_DIM_VEHICLE_SCD2 TARGET
    WHERE TARGET.ID_VEHICLE_BK = SOURCE.ID_VEHICLE
    AND TARGET.FLG_CURRENT = TRUE
    AND TARGET.CAT_MODEL = SOURCE.CAT_MODEL
    AND TARGET.TYP_FUEL_TYPE = SOURCE.TYP_FUEL_TYPE
    AND TARGET.ID_CUSTOMER_CURRENT = SOURCE.ID_CUSTOMER_CURRENT
    AND TARGET.CAT_VEHICLE_STATUS = SOURCE.CAT_VEHICLE_STATUS
  );
  
  GET DIAGNOSTICS v_rows_inserted = ROW_COUNT;
  
  SELECT OBJECT_CONSTRUCT(
    'status', 'SUCCESS',
    'rows_closed', v_rows_closed,
    'rows_inserted', v_rows_inserted
  )
  INTO v_result;
  
  RETURN v_result;
END;
$$;
```

> ✅ **Entregable**: `01-scd2-dimensions.sql` con dimensiones y stored procedures.

## 📊 Desafío 2 – Tabla de Hechos y Modelo Dimensional

### 2.1 Tabla de Hechos de Servicio

```sql
CREATE OR REPLACE TABLE DEV_SILVER_DB.S_SERVICE.TB_FACT_SERVICE_VISIT (
    ID_VISIT NUMBER PRIMARY KEY,
    ID_VEHICLE_SK NUMBER,  -- FK a DIM_VEHICLE_SCD2
    ID_CUSTOMER_SK NUMBER,  -- FK a DIM_CUSTOMER_SCD2
    ID_WORKSHOP NUMBER,
    ID_DATE_SK NUMBER,  -- FK a DIM_DATE
    CAT_SERVICE_TYPE VARCHAR(50),
    QTY_KM_VEHICLE NUMBER,
    AMT_COST DECIMAL(10,2),
    AMT_PARTS_COST DECIMAL(10,2),
    AMT_LABOR_COST DECIMAL(10,2),
    RAT_SATISFACTION NUMBER(3,2),
    FLG_RECOMMENDATION BOOLEAN,
    QTY_PARTS_USED NUMBER,
    AUD_TST_LOAD TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (ID_DATE_SK, ID_VEHICLE_SK)
COMMENT = 'Tabla de hechos de visitas a servicio - Star Schema';
```

### 2.2 Dimensión de Fecha

```sql
CREATE OR REPLACE TABLE DEV_SILVER_DB.S_COMMON.TB_DIM_DATE AS
SELECT
  DATEADD(DAY, SEQ4(), '2020-01-01'::DATE) AS ID_DATE_SK,
  DATEADD(DAY, SEQ4(), '2020-01-01'::DATE) AS DTE_DATE,
  YEAR(DATEADD(DAY, SEQ4(), '2020-01-01'::DATE)) AS NUM_YEAR,
  QUARTER(DATEADD(DAY, SEQ4(), '2020-01-01'::DATE)) AS NUM_QUARTER,
  MONTH(DATEADD(DAY, SEQ4(), '2020-01-01'::DATE)) AS NUM_MONTH,
  DAYOFWEEK(DATEADD(DAY, SEQ4(), '2020-01-01'::DATE)) AS NUM_DAY_OF_WEEK,
  DAYNAME(DATEADD(DAY, SEQ4(), '2020-01-01'::DATE)) AS NAM_DAY_NAME,
  MONTHNAME(DATEADD(DAY, SEQ4(), '2020-01-01'::DATE)) AS NAM_MONTH_NAME,
  CASE 
    WHEN DAYOFWEEK(DATEADD(DAY, SEQ4(), '2020-01-01'::DATE)) IN (0, 6) THEN TRUE 
    ELSE FALSE 
  END AS FLG_WEEKEND,
  CASE 
    WHEN MONTH(DATEADD(DAY, SEQ4(), '2020-01-01'::DATE)) IN (12, 1, 2) THEN 'WINTER'
    WHEN MONTH(DATEADD(DAY, SEQ4(), '2020-01-01'::DATE)) IN (3, 4, 5) THEN 'SPRING'
    WHEN MONTH(DATEADD(DAY, SEQ4(), '2020-01-01'::DATE)) IN (6, 7, 8) THEN 'SUMMER'
    ELSE 'AUTUMN'
  END AS CAT_SEASON
FROM TABLE(GENERATOR(ROWCOUNT => 3653))  -- 10 años
WHERE DATEADD(DAY, SEQ4(), '2020-01-01'::DATE) <= CURRENT_DATE();
```

### 2.3 Vista Star Schema Completa

```sql
CREATE OR REPLACE VIEW DEV_SILVER_DB.S_ANALYTICS.V_STAR_SERVICE_VISIT AS
SELECT
  F.ID_VISIT,
  -- Dimensión Fecha
  D.DTE_DATE,
  D.NUM_YEAR,
  D.NUM_QUARTER,
  D.NUM_MONTH,
  D.CAT_SEASON,
  -- Dimensión Vehículo
  V.ID_VEHICLE_BK,
  V.CAT_MODEL,
  V.TYP_FUEL_TYPE,
  V.DTE_REGISTRATION,
  V.QTY_CURRENT_KM,
  V.CAT_VEHICLE_STATUS,
  -- Dimensión Cliente
  C.ID_CUSTOMER_BK,
  C.NAM_CUSTOMER,
  C.TYP_CUSTOMER_TYPE,
  C.REF_COUNTRY_CODE,
  C.CAT_SEGMENT,
  -- Dimensión Taller
  W.NAM_WORKSHOP,
  W.NAM_CITY,
  W.REF_COUNTRY_CODE AS WORKSHOP_COUNTRY,
  -- Hechos
  F.CAT_SERVICE_TYPE,
  F.QTY_KM_VEHICLE,
  F.AMT_COST,
  F.AMT_PARTS_COST,
  F.AMT_LABOR_COST,
  F.RAT_SATISFACTION,
  F.FLG_RECOMMENDATION,
  F.QTY_PARTS_USED
FROM DEV_SILVER_DB.S_SERVICE.TB_FACT_SERVICE_VISIT F
LEFT JOIN DEV_SILVER_DB.S_COMMON.TB_DIM_DATE D 
  ON F.ID_DATE_SK = D.ID_DATE_SK
LEFT JOIN DEV_SILVER_DB.S_VEHICLE.TB_DIM_VEHICLE_SCD2 V 
  ON F.ID_VEHICLE_SK = V.ID_VEHICLE_SK
LEFT JOIN DEV_SILVER_DB.S_CUSTOMER.TB_DIM_CUSTOMER_SCD2 C 
  ON F.ID_CUSTOMER_SK = C.ID_CUSTOMER_SK
LEFT JOIN DEV_SILVER_DB.S_WORKSHOP.TB_WORKSHOP W 
  ON F.ID_WORKSHOP = W.ID_WORKSHOP;
```

> ✅ **Entregable**: `02-star-schema-model.sql` con modelo dimensional completo.

## 🔄 Desafío 3 – ETL Completo Multi-Tabla

### 3.1 Stored Procedure Principal de ETL

Crea un stored procedure que:

1. Cargue dimensiones con SCD Type 2
2. Cargue tabla de hechos
3. Valide integridad referencial
4. Genere métricas de carga

```sql
CREATE OR REPLACE PROCEDURE SP_LOAD_SERVICE_STAR_SCHEMA(
    P_START_DATE DATE DEFAULT NULL,
    P_END_DATE DATE DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  v_batch_id VARCHAR(100) := 'BATCH_' || TO_CHAR(CURRENT_TIMESTAMP(), 'YYYYMMDDHH24MISS');
  v_result VARIANT;
  v_dim_vehicle_result VARIANT;
  v_dim_customer_result VARIANT;
  v_fact_rows NUMBER;
BEGIN
  -- Paso 1: Cargar dimensión Vehículo (SCD Type 2)
  CALL DEV_SILVER_DB.S_VEHICLE.SP_LOAD_VEHICLE_SCD2(
    'DEV_BRONZE_DB.B_VEHICLE_SYSTEM.TB_VEHICLES_RAW'
  ) INTO v_dim_vehicle_result;
  
  -- Paso 2: Cargar dimensión Cliente (SCD Type 2)
  CALL DEV_SILVER_DB.S_CUSTOMER.SP_LOAD_CUSTOMER_SCD2(
    'DEV_BRONZE_DB.B_CUSTOMER_SYSTEM.TB_CUSTOMERS_RAW'
  ) INTO v_dim_customer_result;
  
  -- Paso 3: Cargar tabla de hechos
  INSERT INTO DEV_SILVER_DB.S_SERVICE.TB_FACT_SERVICE_VISIT (
    ID_VISIT,
    ID_VEHICLE_SK,
    ID_CUSTOMER_SK,
    ID_WORKSHOP,
    ID_DATE_SK,
    CAT_SERVICE_TYPE,
    QTY_KM_VEHICLE,
    AMT_COST,
    AMT_PARTS_COST,
    AMT_LABOR_COST,
    RAT_SATISFACTION,
    FLG_RECOMMENDATION,
    QTY_PARTS_USED
  )
  SELECT
    V.ID_VISIT,
    VE.ID_VEHICLE_SK,
    C.ID_CUSTOMER_SK,
    V.ID_WORKSHOP,
    D.ID_DATE_SK,
    V.CAT_SERVICE_TYPE,
    V.QTY_KM_VEHICLE,
    V.AMT_COST,
    COALESCE(VP.AMT_PARTS_TOTAL, 0) AS AMT_PARTS_COST,
    V.AMT_COST - COALESCE(VP.AMT_PARTS_TOTAL, 0) AS AMT_LABOR_COST,
    V.RAT_SATISFACTION,
    V.FLG_RECOMMENDATION,
    COALESCE(VP.QTY_PARTS_TOTAL, 0) AS QTY_PARTS_USED
  FROM DEV_SILVER_DB.S_SERVICE.TB_SERVICE_VISIT_UNIFIED V
  LEFT JOIN DEV_SILVER_DB.S_VEHICLE.TB_DIM_VEHICLE_SCD2 VE
    ON V.ID_VEHICLE = VE.ID_VEHICLE_BK
    AND V.DTE_VISIT >= VE.DTE_VALID_FROM
    AND (V.DTE_VISIT < VE.DTE_VALID_TO OR VE.DTE_VALID_TO IS NULL)
  LEFT JOIN DEV_SILVER_DB.S_CUSTOMER.TB_DIM_CUSTOMER_SCD2 C
    ON V.ID_CUSTOMER = C.ID_CUSTOMER_BK
    AND V.DTE_VISIT >= C.DTE_VALID_FROM
    AND (V.DTE_VISIT < C.DTE_VALID_TO OR C.DTE_VALID_TO IS NULL)
  LEFT JOIN DEV_SILVER_DB.S_COMMON.TB_DIM_DATE D
    ON DATE(V.DTE_VISIT) = D.DTE_DATE
  LEFT JOIN (
    SELECT 
      ID_VISIT,
      SUM(AMT_LINE_TOTAL) AS AMT_PARTS_TOTAL,
      SUM(QTY_USED) AS QTY_PARTS_TOTAL
    FROM DEV_SILVER_DB.S_SERVICE.TB_VISIT_PARTS
    GROUP BY ID_VISIT
  ) VP ON V.ID_VISIT = VP.ID_VISIT
  WHERE 
    (P_START_DATE IS NULL OR DATE(V.DTE_VISIT) >= P_START_DATE)
    AND (P_END_DATE IS NULL OR DATE(V.DTE_VISIT) <= P_END_DATE)
    AND VE.ID_VEHICLE_SK IS NOT NULL  -- Validar integridad referencial
    AND D.ID_DATE_SK IS NOT NULL;
  
  GET DIAGNOSTICS v_fact_rows = ROW_COUNT;
  
  -- Retornar métricas
  SELECT OBJECT_CONSTRUCT(
    'batch_id', v_batch_id,
    'status', 'SUCCESS',
    'dimension_vehicle', v_dim_vehicle_result,
    'dimension_customer', v_dim_customer_result,
    'fact_rows_inserted', v_fact_rows,
    'execution_timestamp', CURRENT_TIMESTAMP()
  )
  INTO v_result;
  
  RETURN v_result;
END;
$$;
```

> ✅ **Entregable**: `03-etl-multi-table.sql` con stored procedure completo.

## 📈 Desafío 4 – Agregaciones y Cubos Analíticos

### 4.1 Vista Agregada por Múltiples Dimensiones

```sql
CREATE OR REPLACE VIEW DEV_SILVER_DB.S_ANALYTICS.V_SERVICE_CUBE AS
SELECT
  D.NUM_YEAR,
  D.NUM_QUARTER,
  D.CAT_SEASON,
  V.CAT_MODEL,
  V.TYP_FUEL_TYPE,
  C.REF_COUNTRY_CODE,
  C.CAT_SEGMENT,
  W.NAM_CITY,
  F.CAT_SERVICE_TYPE,
  COUNT(DISTINCT F.ID_VISIT) AS QTY_VISITS,
  COUNT(DISTINCT F.ID_VEHICLE_SK) AS QTY_UNIQUE_VEHICLES,
  SUM(F.AMT_COST) AS AMT_TOTAL_COST,
  AVG(F.AMT_COST) AS AMT_AVG_COST,
  SUM(F.AMT_PARTS_COST) AS AMT_TOTAL_PARTS_COST,
  SUM(F.AMT_LABOR_COST) AS AMT_TOTAL_LABOR_COST,
  AVG(F.RAT_SATISFACTION) AS AVG_SATISFACTION,
  SUM(CASE WHEN F.FLG_RECOMMENDATION = TRUE THEN 1 ELSE 0 END) * 100.0 / 
    NULLIF(COUNT(CASE WHEN F.FLG_RECOMMENDATION IS NOT NULL THEN 1 END), 0) AS RECOMMENDATION_RATE_PCT
FROM DEV_SILVER_DB.S_SERVICE.TB_FACT_SERVICE_VISIT F
JOIN DEV_SILVER_DB.S_COMMON.TB_DIM_DATE D ON F.ID_DATE_SK = D.ID_DATE_SK
JOIN DEV_SILVER_DB.S_VEHICLE.TB_DIM_VEHICLE_SCD2 V ON F.ID_VEHICLE_SK = V.ID_VEHICLE_SK
JOIN DEV_SILVER_DB.S_CUSTOMER.TB_DIM_CUSTOMER_SCD2 C ON F.ID_CUSTOMER_SK = C.ID_CUSTOMER_SK
JOIN DEV_SILVER_DB.S_WORKSHOP.TB_WORKSHOP W ON F.ID_WORKSHOP = W.ID_WORKSHOP
GROUP BY 
  D.NUM_YEAR,
  D.NUM_QUARTER,
  D.CAT_SEASON,
  V.CAT_MODEL,
  V.TYP_FUEL_TYPE,
  C.REF_COUNTRY_CODE,
  C.CAT_SEGMENT,
  W.NAM_CITY,
  F.CAT_SERVICE_TYPE;
```

> ✅ **Entregable**: `04-analytical-cube.sql` con vistas agregadas.

## Entregables

Asegúrate de incluir los siguientes archivos en tu entrega:

* `01-scd2-dimensions.sql` – Dimensiones con SCD Type 2 y stored procedures
* `02-star-schema-model.sql` – Modelo dimensional completo (Star Schema)
* `03-etl-multi-table.sql` – Stored procedure de ETL multi-tabla
* `04-analytical-cube.sql` – Vistas agregadas y cubos analíticos
* `lab-notes.md` – Documento que incluya:
  * Explicación de la estrategia SCD Type 2 implementada
  * Diseño del modelo dimensional y justificación
  * Desafíos en la integración multi-tabla
  * Ventajas del modelo Star Schema para análisis
  * Recomendaciones para optimización

## 🎉 Conclusión

Has implementado un modelo dimensional completo con SCD Type 2 y ETL multi-tabla para el servicio postventa de SEAT. Este modelo está optimizado para análisis analíticos complejos y mantiene historial completo de cambios.

¡Excelente trabajo!

