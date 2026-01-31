# ETL Incremental - Right Now

Este proyecto implementa un proceso ETL incremental para procesar datos de la plataforma Right Now de FONASA.

## Estructura del Proyecto

```
ETL_rightnow_DTM_hourly/
├── src/
│   ├── config/
│   │   └── settings.py          # Configuración del proyecto
│   ├── db_manager/              # Gestión de conexiones a BD
│   ├── etl_incremental.py       # Script principal ETL incremental
│   ├── fix_ETL.py               # Script ETL completo (legacy)
│   └── .env                     # Variables de entorno (no versionar)
├── Dockerfile                   # Imagen Docker
├── docker-compose.yml           # Orquestación de servicios
├── requirements.txt             # Dependencias Python
├── .env.example                 # Plantilla de variables de entorno
└── .dockerignore               # Archivos excluidos de Docker
```

## Configuración

### 1. Variables de Entorno

Crea un archivo `.env` en `src/` basándote en `.env.example`:

```bash
cp .env.example src/.env
```

Edita `src/.env` con tus credenciales reales.

### 2. Ejecución con Docker

#### Construir la imagen

```bash
docker-compose build
```

#### Ejecutar el ETL (una vez)

```bash
docker-compose run --rm etl_incremental
```

#### Ejecutar con base de datos local (para testing)

```bash
docker-compose up
```

### 3. Ejecución Local (sin Docker)

```bash
# Instalar dependencias
pip install -r requirements.txt

# Ejecutar ETL
python src/etl_incremental.py
```

## Programación Horaria

### Opción 1: Cron (Linux/Mac)

Agrega a tu crontab:

```cron
0 * * * * cd /ruta/proyecto && docker-compose run --rm etl_incremental
```

### Opción 2: Task Scheduler (Windows)

Crea una tarea programada que ejecute:

```batch
docker-compose -f "d:\Users\mailj\OneDrive\Trabajos\MedIQ\Proyectos\202508_RightNowWebScraping\ETL_rightnow_DTM_hourly\docker-compose.yml" run --rm etl_incremental
```

### Opción 3: Kubernetes CronJob

Para entornos productivos, usa un CronJob de Kubernetes.

## Arquitectura del ETL Incremental

### Tablas Procesadas

1. **Historia (Append-only)**
   - `dm_rn_fechas_folio_padre`: Cambios de estado de folios padre
   - `dm_rn_fechas_folio_hijo`: Cambios de estado de folios hijo

2. **Dimensiones (UPSERT)**
   - `dm_rn_folio_padre`: Estado actual + metadata de folios padre
   - `dm_rn_folio_hijo`: Estado actual + metadata de folios hijo

### Flujo de Proceso

1. **Extracción Incremental**: Lee solo registros nuevos desde `rn_registro_casos_transi`
2. **Detección de Cambios**: Compara con último estado conocido
3. **Suprimidos**: Identifica folios ausentes en el snapshot actual
4. **Actualización**: UPSERT en tablas de dimensiones

## Logs

Los logs se muestran en stdout. Para persistirlos:

```bash
docker-compose run --rm etl_incremental 2>&1 | tee etl_$(date +%Y%m%d_%H%M%S).log
```

## Troubleshooting

### Error de conexión a PostgreSQL

- Verifica que `PG_HOST` en `.env` sea accesible desde el contenedor
- Si usas `localhost`, cámbialo a la IP de tu red local o usa `host.docker.internal` (Docker Desktop)

### Errores de columnas faltantes

- El script valida la existencia de columnas requeridas
- Revisa logs para identificar qué columnas faltan en `rn_registro_casos_transi`
