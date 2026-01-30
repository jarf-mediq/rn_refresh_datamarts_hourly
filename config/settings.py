import os
import dotenv; dotenv.load_dotenv()

URL_LOGIN = 'https://fonasa.custhelp.com/app/utils/login_form/redirect/home_contacto'
WEB_URL= 'https://fonasa.custhelp.com/app/utils/login_form/redirect/home_contacto'

GES_NO_GES_URL = "https://fonasa.custhelp.com/app/account/questions/list_pi_le"

USERNAME_SELECTOR = '//*[@id="rn_LoginForm_22_Username"]'
PASSWORD_SELECTOR = '//*[@id="rn_LoginForm_22_Password"]'
LOGIN_BUTTON_SELECTOR = '//*[@id="rn_LoginForm_22_Submit"]'

def get_credentials(codHospital: str):
    USERNAME = os.getenv(f"WEB_USER_{codHospital}")
    PASSWORD = os.getenv(f"WEB_PASS_{codHospital}")
    return USERNAME, PASSWORD

# para navegacion a reporte GES/NOGES
REPORTS_MENU_SELECTOR = '//*[@id="yui_3_18_1_1_1759422677921_807"]'
REPORT_MENU_URL_READY = "https://fonasa.custhelp.com/app/account/questions/pago/list/p/1/org/2" # url de menu 

# Configura parametros del navegador
HEADLESS_BROWSER = True
BROWSER_TYPE = "chromium"

# Configuración de timeouts (opcional)
DOWNLOAD_TIMEOUT = 1000*60*3  # 3 minutos en milisegundos
BROWSER_TIMEOUT = 1000*60*3  # 3 minutos en milisegundos
FICHA_TIMEOUT = 1000*3  # 3 segundos en milisegundos
MAX_RETRIES = 3
RETRY_DELAY = 5  # segundos entre reintentos

# carpeta para archivos temporales
TEMP_FOLDER = './data/temp'
DATA_FOLDER = r'F:\MedIQ\Respaldo\RightNow-FONASA\DetalleCasos'

##MANEJO DE CLIENTES
LISTA_CLIENTES_ID = ['107223','107224', '111202', '200842', '201573']
#LISTA_CLIENTES_ID = ['111202']
#LISTA_CLIENTES_ID_TEST = ['111202']

# lote de fichas a procesar simultaneamente
LOTE_FICHAS = 5

##BASES DE DATOS
# credenciales de la base de datos
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_HOST = os.getenv('PG_HOST')
PG_PORT = os.getenv('PG_PORT')
PG_DB_NAME = os.getenv('PG_DB_NAME')
POSTGRES_URI = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB_NAME}"

TABLA_REGISTRO_CASOS = 'rn_registro_casos_transi'
TABLA_DETALLE_CASOS = 'rn_detalle_casos_info_general'
TABLA_PRESTACIONES_CASOS = 'rn_detalle_casos_prestaciones'
TABLA_ADJUNTOS_CASOS = 'rn_detalle_casos_adjuntos'
TABLA_COMUNICACIONES_CASOS = 'rn_detalle_casos_comunicaciones'

##MANEJO DE DATOS
ESTADOS_FOLIOS_HIJO_RELEVANTES = ['','Aceptado', 'Rechazado', 'Codificado', 'OC pendiente', 'En Revisión']