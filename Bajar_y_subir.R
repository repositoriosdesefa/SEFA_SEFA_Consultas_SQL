####################################################################-
##########  Consultas SQL a bases de Oracle a través de R  #########-
############################# By LE ################################-

################ I. Librerías, drivers y directorio ################

# I.1 Librerías ----

# i) RJDBC
#install.packages("DBI")
library(DBI)
#install.packages("rJava")
library(rJava)
#install.packages("RJDBC")
library(RJDBC)


# ii) Librerias complementarias
#install.packages("googledrive")
library(googledrive)
#install.packages("googlesheets4")
library(googlesheets4)
#install.packages("httpuv")
library(httpuv)
#install.packages("purrr")
library(purrr)
#install.packages("blastula")
library(blastula)
#install.packages("lubridate")
library(lubridate)
#install.packages("stringr")
library(stringr)

# I.2 Drivers

# i) Oracle
# Driver OJDBC
rutaDriver <- ""
oracleDriver <- JDBC("oracle.jdbc.OracleDriver",
                     classPath=rutaDriver)
#*El driver debe estar descargado y en una ubicación fija

# ii) Google
correo_usuario <- ""
drive_auth(email = correo_usuario) 
gs4_auth(token = drive_auth(email = correo_usuario), 
         email = correo_usuario)
#*El token debe estar almacenado y con los permisos de Google

# I.3 Directorio

# i) Local
directorio <- ""
consulta_dir <- file.path(directorio, "Consultas")
#*Establecer el directorio donde se encuentran las consultas

# ii) Drive de SEFA
dirSEFA_drive <- ""
dirSEFA <- read_sheet(ss = dirSEFA_drive)
# División en armadas
dirSEFA1 <- dirSEFA[1:3,] # SINADA, SISEFA y Reporte Minero
dirSEFA2 <- dirSEFA[4:10,] # PLANEFA
dirSEFA3 <- dirSEFA[11:16,] # Reporte Trimestral
dirSEFA4 <- dirSEFA[17:19,] # Reporte Trimestral Acciones de supervisión
dirSEFA5 <- dirSEFA[20:26,] # Reporte Trimestral Acciones Evaluación, IN y PAS, y RR
#*La matriz debe contener los IDs correctos para la carga

#-----------------------------------------------------------------

################ II. Establecimiento de conexión ################

# II.1 Credenciales ----
usuario <- ""
clave <- ""
hostSEFA <- ""
hostSISEFA <- ""
#*Información sensible y privada
usuario_RR <- ""
clave_RR <- ""
hostSEFA_RR <- ""

# II.2 Conexión
# Tablas principales de SEFA
conexionSEFA <- dbConnect(oracleDriver, hostSEFA,
                          usuario, clave)
# Tabla de SISEFA
conexionSISEFA <- dbConnect(oracleDriver, hostSISEFA,
                            usuario, clave)
# Tabla de RR
conexionSEFA_RR <- dbConnect(oracleDriver, hostSEFA_RR,
                             usuario_RR, clave_RR)
# Vector de conexiones de SEFA
conexionOEFA <- c(conexionSEFA, conexionSISEFA, conexionSEFA_RR)

#*Se debe contar con credenciales para establecer la conexión

#-----------------------------------------------------------------

############## III. Descarga y carga de información ##############

# III.1 Funciones ----

# i) Lectura de SQL
getSQL <- function(filepath){
  con = file(filepath, "r")
  sql.string <- ""
  while (TRUE){
    line <- readLines(con, n = 1)
    if ( length(line) == 0 ){
      break
    }
    line <- gsub("\\t", " ", line)
    if(grepl("--",line) == TRUE){
      line <- paste(sub("--","/*",line),"*/")
    }
    sql.string <- paste(sql.string, line)
  }
  close(con)
  return(sql.string)
}

# ii) Función de descarga y carga de información
baja_y_sube <- function(consulta, ID, hoja){
  
  # Condicionales para elección de Host
  if(consulta == "SISEFA.sql") {
    conexion = conexionOEFA[[2]]
  } else if (consulta == "RR.sql") {
    conexion = conexionOEFA[[3]]
  } else {
    conexion = conexionOEFA[[1]]
  }
  
  consulta_ruta = file.path(consulta_dir, consulta)
  query = getSQL(consulta_ruta)
  datos = dbGetQuery(conexion, query)
  write_sheet(datos, ID, hoja)
  # hoja_rango = paste0("'",hoja, "'!A2")
  # range_write(ID, data = datos,
  #             range = hoja_rango,
  #             col_names = F)
}

# iii) Función robustecida de descarga y carga de información
R_baja_y_sube <- function(consulta, ID, hoja){
  out = tryCatch(baja_y_sube(consulta, ID, hoja),
                 error = function(e){
                   baja_y_sube(consulta, ID, hoja) 
                 })
  return(out)
}

# III.2 Descarga y carga de información ----
#Iteración sobre los argumentos definidos en el dataframe
# i) Primera armada
pwalk(list(dirSEFA1$Consulta, dirSEFA1$ID, dirSEFA1$Hoja),
      slowly(R_baja_y_sube, rate_backoff(10, max_times = Inf)))
# ii) Segunda armada
pwalk(list(dirSEFA2$Consulta, dirSEFA2$ID, dirSEFA2$Hoja),
      slowly(R_baja_y_sube, rate_backoff(10, max_times = Inf)))
# ii) Tercera armada
pwalk(list(dirSEFA3$Consulta, dirSEFA3$ID, dirSEFA3$Hoja),
      slowly(R_baja_y_sube, rate_backoff(10, max_times = Inf)))
# ii) Cuarta armada
pwalk(list(dirSEFA4$Consulta, dirSEFA4$ID, dirSEFA4$Hoja),
      slowly(R_baja_y_sube, rate_backoff(10, max_times = Inf)))
# ii) Quinta armada
pwalk(list(dirSEFA5$Consulta, dirSEFA5$ID, dirSEFA5$Hoja),
      slowly(R_baja_y_sube, rate_backoff(10, max_times = Inf)))

# III.3 Cierre de conexión
dbDisconnect(conexionSEFA)
dbDisconnect(conexionSISEFA)

#-----------------------------------------------------------------

################# IV. Envío de correo automático #################

# IV.O Email: Credenciales ----
#install.packages("keyring")
#library(keyring)
# Mi_email <- ""
# create_smtp_creds_key(
#   Mi_email,
#   user = Mi_email,
#   provider = "gmail",
#   use_ssl = TRUE,
#   overwrite = TRUE
# )

# IV.1 Email: Cabecera ----
Arriba <- add_image(
  file = "https://i.imgur.com/WxwYdaY.png",
  width = 1000,
  align = c("right"))
Cabecera <- md(Arriba)

# IV.2 Email: Pie de página ----
Logo_Oefa <- add_image(
  file = "https://i.imgur.com/ImFWSQj.png",
  width = 280)
Pie_de_pagina <- blocks(
  md(Logo_Oefa),
  block_text(md("Av. Faustino Sánchez Carrión N.° 603, 607 y 615 - Jesús María"), align = c("center")),
  block_text(md("Teléfonos: 204-9900 Anexo 7154"), align = c("center")),
  block_text("www.oefa.gob.pe", align = c("center")),
  block_text(md("**Síguenos** en nuestras redes sociales"), align = c("center")),
  block_social_links(
    social_link(
      service = "Twitter",
      link = "https://twitter.com/OEFAperu",
      variant = "dark_gray"
    ),
    social_link(
      service = "Facebook",
      link = "https://www.facebook.com/oefa.peru",
      variant = "dark_gray"
    ),
    social_link(
      service = "Instagram",
      link = "https://www.instagram.com/somosoefa/",
      variant = "dark_gray"
    ),
    social_link(
      service = "LinkedIn",
      link = "https://www.linkedin.com/company/oefa/",
      variant = "dark_gray"
    ),
    social_link(
      service = "YouTube",
      link = "https://www.youtube.com/user/OEFAperu",
      variant = "dark_gray"
    )
  ),
  block_spacer(),
  block_text(md("Imprime este correo electrónico sólo si es necesario. Cuidar el ambiente es responsabilidad de todos."), align = c("center"))
)

# IV.3 Email: Cuerpo del mensaje ----

# i) Botón de base de datos
cta_button <- add_cta_button(
  url = "https://drive.google.com/drive/folders/1NB620MUArYqmYWT1lCwzozICUDM7_4so",
  text = "Bases de datos"
)

# ii) Texto del correo
Cuerpo_del_mensaje <- blocks(
  md("
Buenos días,

Se ha actualizado satisfactoriamente las bases de datos de los siguientes aplicativos de SEFA: 
* PLANEFA
* Reporte Trimestral
* Reporte Minero
* SISEFA
* SINADA 
* Reporta Residuos

Pueden acceder a las bases de datos actualizadas desde el siguiente enlace:"
  ),
  md(c(cta_button)),
  md("

***
**Tener en cuenta:**
- Este correo electrónico ha sido generado de manera automática a través de R.
- El uso de lenguajes de programación de alto nivel (R, Python y SQL) para facilitar el trabajo realizado en SEFA es parte de un proyecto impulsado desde la Subdirección.
- En caso de no querer recibir este correo, por favor, comuníquese a proyectossefa@oefa.gob.pe
     ")
)

# iii) Destinatarios 
Destinatarios <- c(

)

Destinatarios_cc <- c(

)

Destinatarios_bcc <- c(

)

# iv) Asunto
mes_actual <- month(now(), label=TRUE, abbr = FALSE)
mes_actual <- str_to_lower(mes_actual)

Asunto <- paste("Actualización diaria de aplicativos de SEFA | ", 
                day(now())," de ", mes_actual, " de ", year(now()))

#V. 4 Email: Composición ----
email <- compose_email(
  header = Cabecera,
  body = Cuerpo_del_mensaje, 
  footer = Pie_de_pagina,
  content_width = 1000
)

# IV.5 Email: Envío ----
smtp_send(
  email,
  to = Destinatarios,
  from = c("Equipo de Proyectos e Innovación" = ""),
  subject = Asunto,
  cc = Destinatarios_cc,
  bcc = Destinatarios_bcc,
  credentials = creds_key(id = ""),
  verbose = TRUE
)
