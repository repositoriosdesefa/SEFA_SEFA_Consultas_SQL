# SEFA: Consultas SQL
El proyecto Consultas SQL extrae información de los aplicativos de la Subdirección de Seguimiento de Entidades de Fiscalización Ambiental (SEFA), a fin de que puedan ser procesados para el análisis de datos y como fuentes de información para los tablero de control que maneja la subdirección.

La extracción de datos de realiza diariamente, la cual se notifica a través de un correo, de los siguientes aplicativos: 
1. PLANEFA
2. Reporte Trimestral
3. SINADA
4. SISEFA
5. Reporte Minero
6. Reporta Residuos.

# Archivos
- Bajar_y_subir.R : Script en R para la descarga de información de bases de Oracle y subida de información a Google Sheets.
- Cabecera.png : Cabecera de correo automatizado.

# Observaciones
- Fueron eliminados los objetos que contenían información sensible como credenciales de acceso, ID de archivos de Google Sheets y rutas locales.
