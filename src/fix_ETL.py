from datetime import date, timedelta

import pandas as pd
import numpy as np
import sqlalchemy
import config.settings as settings

engine = sqlalchemy.create_engine(settings.POSTGRES_URI)

qry = """
SELECT * FROM rn_registro_casos_transi
"""
# Ahora el df_global ya viene con la lógica de folios suprimidos y cambios de estado
df_global = pd.read_sql_query(qry, engine)

print(df_global)

# carga datos de registro de casos detalles: información general del caso
tabla="rn_detalle_casos_info_general"
qry=f"select * from {tabla}"
df_info_general = pd.read_sql_query(qry, engine)

# carga datos de registro de casos detalles: información general del caso
tabla="rn_detalle_casos_prestaciones"
qry=f"select * from {tabla}"
df_prestaciones = pd.read_sql_query(qry, engine)


### Construye tabla: Fecha Folio Padre
# ordena folios por cliente y fecha de registro
dm_rn_fechas_folio_padre= df_global[["cliente","folio_padre","fecha_registro","estado_padre"]].sort_values(by=["cliente","fecha_registro"])
# # elimina duplicados por cliente, folio_padre y estado_padre (solo queda un estado por folio con la fecha de la primera aparición)
# dm_rn_fechas_folio_padre=dm_rn_fechas_folio_padre.drop_duplicates(subset=["cliente","folio_padre","estado_padre"],keep="first").reset_index(drop=True).sort_values(by=["cliente","folio_padre","fecha_registro"])
dm_rn_fechas_folio_padre=dm_rn_fechas_folio_padre.sort_values(by=["cliente","folio_padre","fecha_registro"])

# Filtra solo cuando cambia estado_padre dentro de cada (cliente, folio_padre)
dm_rn_fechas_folio_padre = dm_rn_fechas_folio_padre[
    dm_rn_fechas_folio_padre.groupby(['cliente', 'folio_padre'])['estado_padre']
    .shift() != dm_rn_fechas_folio_padre['estado_padre']
].reset_index(drop=True)

# agregar estado "Folio suprimido" para los folio que ya no están en la lista
suprimidos_df = df_global.groupby(['cliente', 'folio_padre'])['fecha_registro'].agg(['max']).reset_index().rename(columns={'max':'fecha_registro'})
fecha_hoy=date.today()-timedelta(days=0) # la referencia debe ser el día 18/11/2025
suprimidos_df=suprimidos_df[suprimidos_df["fecha_registro"].dt.date != fecha_hoy].reset_index(drop=True)
suprimidos_df["estado_padre"]="Folio suprimido"
suprimidos_df.drop_duplicates(subset=["cliente","folio_padre","estado_padre"],keep="first",inplace=True)

# agregar folios con estado "Folio suprimido"
dm_rn_fechas_folio_padre=pd.concat([dm_rn_fechas_folio_padre,suprimidos_df],ignore_index=True).sort_values(by=["cliente","folio_padre","fecha_registro"])

dm_rn_fechas_folio_padre.to_sql("dm_rn_fechas_folio_padre", engine, if_exists="replace", index=False)


### Construye tabla: Fecha Folio Hijo
dm_rn_fechas_folio_hijo= df_global[["cliente","folio_padre","folio_hijo","ppa_grd","fecha_registro","estado_padre", "estado_hijo"]].sort_values(by=["cliente","fecha_registro"])
dm_rn_fechas_folio_hijo=dm_rn_fechas_folio_hijo[~dm_rn_fechas_folio_hijo["folio_hijo"].isin(["", None])].sort_values(by=["cliente","folio_hijo","fecha_registro"])

# Filtra solo cuando cambia estado_hijo dentro de cada (cliente, folio_hijo)
dm_rn_fechas_folio_hijo = dm_rn_fechas_folio_hijo[
    dm_rn_fechas_folio_hijo.groupby(['cliente', 'folio_hijo'])['estado_hijo']
    .shift() != dm_rn_fechas_folio_hijo['estado_hijo']
].reset_index(drop=True)

# agregar estado "Folio suprimido" para los folio que ya no están en la lista
suprimidos_df = df_global.groupby(['cliente', 'folio_padre', 'folio_hijo'])['fecha_registro'].agg(['max']).reset_index().rename(columns={'max':'fecha_registro'})

fecha_hoy=date.today()-timedelta(days=0)
suprimidos_df=suprimidos_df[suprimidos_df["fecha_registro"].dt.date != fecha_hoy].reset_index(drop=True)
suprimidos_df["estado_hijo"]="Folio suprimido"
suprimidos_df.drop_duplicates(subset=["cliente","folio_hijo","estado_hijo"],keep="first",inplace=True)

# agregar folios con estado "Folio suprimido"
dm_rn_fechas_folio_hijo=pd.concat([dm_rn_fechas_folio_hijo,suprimidos_df],ignore_index=True).sort_values(by=["cliente","folio_padre","folio_hijo","fecha_registro"])

# pasa las tablas a postgres
dm_rn_fechas_folio_hijo.to_sql("dm_rn_fechas_folio_hijo", engine, if_exists="replace", index=False)

########################################################
### Construye tabla: Folio Padre
dm_rn_folio_padre= df_global[["cliente","folio_padre","fecha_asignacion","rut_paciente","nombre_paciente","intervencion_sanitaria","url_ficha"]]
dm_rn_folio_padre["fecha_asignacion"]=pd.to_datetime(dm_rn_folio_padre["fecha_asignacion"])

# Excluye los registros con url_ficha que contienen "codificado"
dm_rn_folio_padre=dm_rn_folio_padre[~dm_rn_folio_padre["url_ficha"].str.contains("codificado")].drop_duplicates(subset=["cliente","folio_padre"]).reset_index(drop=True)

# Separa los registros entre los que fueron creados posterior a la fecha en que se inició el seguiemiento con el scraping (current) y los que fueron creados antes (legacy)
# obtene la referencia de la primera fecha de registro para cada cliente
primer_registro_cliente = df_global.groupby('cliente')['fecha_registro'].min().reset_index().rename(columns={'fecha_registro': 'fecha_primer_registro'})

dm_rn_folio_padre_current=dm_rn_folio_padre[(dm_rn_folio_padre.apply(lambda row: row["fecha_asignacion"] >= primer_registro_cliente.loc[primer_registro_cliente["cliente"] == row["cliente"], "fecha_primer_registro"].values[0], axis=1))].reset_index(drop=True)
dm_rn_folio_padre_current["tipo_seguimiento"]="current"

# asignación de fecha de aceptación para registros current: la fecha del primer estado disponible superior a "Asignado" (estados de asignación) y que no sea "Folio suprimido", "SIN Prestador", "Rechazo paciente sin consulta", "No acepta tratamiento" (estados de rechazo)
dm_rn_fechas_folio_padre_primer_estado_current=dm_rn_fechas_folio_padre[(~dm_rn_fechas_folio_padre["estado_padre"].isin(["Asignado"])) & (~dm_rn_fechas_folio_padre["estado_padre"].isin(["Folio suprimido", "SIN Prestador", "Rechazo paciente sin consulta", "No acepta tratamiento"]))]
dm_rn_fechas_folio_padre_primer_estado_current=dm_rn_fechas_folio_padre_primer_estado_current[(dm_rn_fechas_folio_padre_primer_estado_current["fecha_registro"]==dm_rn_fechas_folio_padre_primer_estado_current.groupby(['cliente', 'folio_padre'])['fecha_registro'].transform('min'))]

# asigna la fecha del primer estado disponible en los datos para cada folio padre como fecha de aceptación (utiliza estados posteriores a "Asignado")
dm_rn_folio_padre_current=pd.merge(dm_rn_folio_padre_current, dm_rn_fechas_folio_padre_primer_estado_current[["cliente","folio_padre","fecha_registro"]], on=["cliente","folio_padre"], how="left")
dm_rn_folio_padre_current.rename(columns={"fecha_registro":"fecha_asignacion_aceptada"}, inplace=True)

# asignación de fecha de aceptación para registros legacy:
dm_rn_folio_padre_legacy=dm_rn_folio_padre[(dm_rn_folio_padre.apply(lambda row: row["fecha_asignacion"] < primer_registro_cliente.loc[primer_registro_cliente["cliente"] == row["cliente"], "fecha_primer_registro"].values[0], axis=1))].reset_index(drop=True)
dm_rn_folio_padre_legacy["tipo_seguimiento"]="legacy"

# proporción de códigos identificables: aquellos a los que se les pudo registrar un estado de "Asignado"
dm_rn_fechas_folio_padre_legacy=dm_rn_fechas_folio_padre[dm_rn_fechas_folio_padre["folio_padre"].isin(dm_rn_folio_padre_legacy["folio_padre"])]
folios_identificables_legacy=dm_rn_fechas_folio_padre_legacy[(dm_rn_fechas_folio_padre_legacy["estado_padre"]=="Asignado")][["cliente","folio_padre"]].drop_duplicates()

dm_rn_fechas_folio_padre_legacy_identificables=dm_rn_fechas_folio_padre_legacy[(dm_rn_fechas_folio_padre_legacy["cliente"].isin(folios_identificables_legacy["cliente"])) & (dm_rn_fechas_folio_padre_legacy["folio_padre"].isin(folios_identificables_legacy["folio_padre"]))]

dm_rn_fechas_folio_padre_legacy_identificables_primer_estado=dm_rn_fechas_folio_padre_legacy_identificables[(dm_rn_fechas_folio_padre_legacy_identificables["estado_padre"]!="Asignado") & (dm_rn_fechas_folio_padre_legacy_identificables["fecha_registro"]==dm_rn_fechas_folio_padre_legacy_identificables.groupby(['cliente', 'folio_padre'])['fecha_registro'].transform('min'))]

# asigna la fecha del primer estado disponible en los datos para cada folio padre como fecha de aceptación (utiliza estados posteriores a "Asignado")
dm_rn_folio_padre_legacy=pd.merge(dm_rn_folio_padre_legacy, dm_rn_fechas_folio_padre_legacy_identificables_primer_estado[["cliente","folio_padre","fecha_registro"]], on=["cliente","folio_padre"], how="left")
dm_rn_folio_padre_legacy.rename(columns={"fecha_registro":"fecha_asignacion_aceptada"}, inplace=True)

# para los que no se pudo identificar un estado de "Asignado", se asigna la fecha de asignación como fecha de aceptación
# Para cada valor de "fecha_asignacion_aceptada" que está vacía, asigna el valor de "fecha_asignacion"
dm_rn_folio_padre_legacy["fecha_asignacion_aceptada"] = dm_rn_folio_padre_legacy["fecha_asignacion_aceptada"].fillna(dm_rn_folio_padre_legacy["fecha_asignacion"])

# fuciona los dataframes de current y legacy
dm_rn_folio_padre_final=pd.concat([dm_rn_folio_padre_current, dm_rn_folio_padre_legacy], ignore_index=True)

# carga datos complementarios que están dentro de la ficha del folio padre (info general)
df_info_general_validos=df_info_general[["url_ficha","fecha_atencion","intervencion_sanitaria_ingreso","problema_salud"]].drop_duplicates(subset=["url_ficha"],keep="last")
dm_rn_folio_padre_final_info_general=pd.merge(dm_rn_folio_padre_final, df_info_general_validos, on=["url_ficha"], how="left")

# Agrega dato del ultimo estado del Folio Padre
ultimo_estado_folio_padre=dm_rn_fechas_folio_padre.sort_values(by=["cliente", "folio_padre","fecha_registro"]).drop_duplicates(subset=["cliente", "folio_padre"],keep="last")[["cliente", "folio_padre", "estado_padre","fecha_registro"]]
dm_rn_folio_padre_final_info_general=pd.merge(dm_rn_folio_padre_final_info_general, ultimo_estado_folio_padre, on=["cliente", "folio_padre"], how="left")
dm_rn_folio_padre_final_info_general.rename(columns={"estado_padre":"ultimo_estado_folio_padre", "fecha_registro":"fecha_ultimo_estado_folio_padre"}, inplace=True)

# pasa la tabla a postgres
dm_rn_folio_padre_final_info_general.to_sql("dm_rn_folio_padre", engine, if_exists="replace", index=False)

########################################################
### Construye tabla: Folio Hijo
dm_rn_folio_hijo=df_global[["cliente","folio_padre","folio_hijo","intervencion_sanitaria","ppa_grd","monto_total","url_ficha"]].drop_duplicates(subset=["cliente","folio_padre","folio_hijo"],keep="last")
dm_rn_folio_hijo=dm_rn_folio_hijo[~dm_rn_folio_hijo["folio_hijo"].isin(["", None])].reset_index(drop=True)

# agreaga la información de las fichas de casos
# df_prestaciones_validos=df_prestaciones[["cliente","folio","descripcion","monto_prestacion","monto_at","estado"]].drop_duplicates(subset=["folio"],keep="last")
df_prestaciones_validos=df_prestaciones[["cliente","folio","descripcion","estado"]].drop_duplicates(subset=["folio"],keep="last")
df_prestaciones_validos.rename(columns={"estado":"estado_prestacion"}, inplace=True)

dm_rn_folio_hijo_prestaciones=pd.merge(dm_rn_folio_hijo, df_prestaciones_validos, left_on=["cliente","folio_hijo"], right_on=["cliente","folio"], how="left").drop(columns=["folio"])
dm_rn_folio_hijo_prestaciones_tipo_seguimiento=pd.merge(dm_rn_folio_hijo_prestaciones,dm_rn_folio_padre_final_info_general[["cliente","folio_padre","tipo_seguimiento"]], on=["cliente","folio_padre"], how="left")

# dm_rn_folio_hijo_prestaciones_tipo_seguimiento["monto_total"]=dm_rn_folio_hijo_prestaciones_tipo_seguimiento["monto_prestacion"]+dm_rn_folio_hijo_prestaciones_tipo_seguimiento["monto_at"]
# dm_rn_folio_hijo_prestaciones_tipo_seguimiento=dm_rn_folio_hijo_prestaciones_tipo_seguimiento[['cliente', 'folio_padre', 'folio_hijo', 'intervencion_sanitaria','ppa_grd', 'descripcion', 'monto_prestacion', 'monto_at', 'monto_total', 'estado_prestacion', 'tipo_seguimiento', 'url_ficha']]

# agrega información del ultimo estado del Folio Hijo
ultimo_estado_folio_hijo=dm_rn_fechas_folio_hijo.sort_values(by=["cliente", "folio_padre","folio_hijo","fecha_registro"]).drop_duplicates(subset=["cliente", "folio_padre","folio_hijo"],keep="last")[["cliente", "folio_padre","folio_hijo", "estado_hijo","fecha_registro"]]
dm_rn_folio_hijo_prestaciones_tipo_seguimiento=pd.merge(dm_rn_folio_hijo_prestaciones_tipo_seguimiento, ultimo_estado_folio_hijo, on=["cliente", "folio_padre","folio_hijo"], how="left")
dm_rn_folio_hijo_prestaciones_tipo_seguimiento.rename(columns={"estado_hijo":"ultimo_estado_folio_hijo", "fecha_registro":"fecha_ultimo_estado_folio_hijo"}, inplace=True)

# agrega información del primer estado del Folio Hijo
primer_estado_folio_hijo=dm_rn_fechas_folio_hijo.sort_values(by=["cliente", "folio_padre","folio_hijo","fecha_registro"]).drop_duplicates(subset=["cliente", "folio_padre","folio_hijo"],keep="first")[["cliente", "folio_padre","folio_hijo", "estado_hijo","fecha_registro"]]
dm_rn_folio_hijo_prestaciones_tipo_seguimiento=pd.merge(dm_rn_folio_hijo_prestaciones_tipo_seguimiento, primer_estado_folio_hijo, on=["cliente", "folio_padre","folio_hijo"], how="left")
dm_rn_folio_hijo_prestaciones_tipo_seguimiento.rename(columns={"estado_hijo":"primer_estado_folio_hijo", "fecha_registro":"fecha_primer_estado_folio_hijo"}, inplace=True)

# formatea grd
dm_rn_folio_hijo_prestaciones_tipo_seguimiento["grd"]=np.where(dm_rn_folio_hijo_prestaciones_tipo_seguimiento["ppa_grd"]=="GRD", dm_rn_folio_hijo_prestaciones_tipo_seguimiento["descripcion"].str.split(":").str[1].str.strip().str.zfill(6), None)

# dm_rn_folio_hijo_prestaciones_tipo_seguimiento.to_parquet(r"E:\MedIQ\Respaldo\RightNow-FONASA\Dashboard\dm_rn_folio_hijo.parquet")
dm_rn_folio_hijo_prestaciones_tipo_seguimiento.to_sql("dm_rn_folio_hijo", engine, if_exists="replace", index=False)