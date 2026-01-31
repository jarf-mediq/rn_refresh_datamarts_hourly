import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import pandas as pd
import numpy as np
import sqlalchemy
from sqlalchemy import text
import config.settings as settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class IncrementalETL:
    def __init__(self, db_uri: str):
        """
        Initialize the ETL process with database connection.
        
        Args:
            db_uri: SQLAlchemy connection string for the database.
        """
        self.engine = sqlalchemy.create_engine(db_uri)
        
    def get_max_date(self, table_name: str, date_column: str = "fecha_registro") -> Optional[datetime]:
        """
        Get the maximum date available in the destination table to determine the watermark.
        
        Args:
            table_name: Name of the table to check.
            date_column: Name of the date column.
            
        Returns:
            Maximum datetime found or None if table is empty/doesn't exist.
        """
        try:
            # Check if table exists first to avoid errors on fresh run
            insp = sqlalchemy.inspect(self.engine)
            if not insp.has_table(table_name):
                logger.warning(f"Table {table_name} does not exist. Returning None.")
                return None

            query = text(f"SELECT MAX({date_column}) FROM {table_name}")
            with self.engine.connect() as conn:
                result = conn.execute(query).scalar()
            
            logger.info(f"Max date for {table_name}: {result}")
            return result
        except Exception as e:
            logger.error(f"Error getting max date from {table_name}: {e}")
            raise

    def extract_new_data(self, start_date: Optional[datetime]) -> pd.DataFrame:
        """
        Extract data from the source table that is newer than start_date.
        
        Args:
            start_date: The watermark date. If None, extracts everything.
            
        Returns:
            DataFrame with new records.
        """
        base_query = "SELECT * FROM rn_registro_casos_transi"
        
        if start_date:
            # Add a small buffer or use > to avoid duplicates if possible, 
            # but given 'hourly' nature and potential concurrent writes, >= might be safer with dedup later
            # However, prompt asks for "datos incrementales".
            
            # Formating date for SQL
            query = f"{base_query} WHERE fecha_registro > '{start_date}'"
        else:
            query = base_query
            
        logger.info(f"Executing extraction query: {query}")
        try:
            df = pd.read_sql_query(query, self.engine)
            logger.info(f"Extracted {len(df)} rows.")
            return df
        except Exception as e:
            logger.error(f"Error extracting data: {e}")
            raise

    def get_active_state_from_db(self, table_name: str, keys: List[str], state_col: str) -> pd.DataFrame:
        """
        Fetch the last known state for folios to calculate transitions correctly.
        """
        # This function will be needed to merge 'previous state' with 'new data'
        # to detect changes like: Status A -> Status B
        # query logic to be implemented
        pass

    def get_latest_state_from_db(self, table_name: str, group_cols: List[str], state_col: str) -> pd.DataFrame:
        """
        Fetch the last known state for each group (client/folio) from the destination table.
        """
        try:
            # Check if table exists
            insp = sqlalchemy.inspect(self.engine)
            if not insp.has_table(table_name):
                return pd.DataFrame()

            # Query to get the last row for each group
            # We use distinct on group_cols sorted by date desc
            cols = ", ".join(group_cols + [state_col, "fecha_registro"])
            distinct_on = ", ".join(group_cols)
            
            # Using Postgres DISTINCT ON to get the last record strictly
            query = f"""
            SELECT DISTINCT ON ({distinct_on}) *
            FROM {table_name}
            ORDER BY {distinct_on}, fecha_registro DESC
            """
            
            df = pd.read_sql_query(query, self.engine)
            return df
        except Exception as e:
            logger.error(f"Error fetching latest state from {table_name}: {e}")
            return pd.DataFrame()

    def process_status_changes(self, df_new: pd.DataFrame, df_db_state: pd.DataFrame, 
                             group_cols: List[str], state_col: str) -> pd.DataFrame:
        """
        Detect status changes by comparing new data with previous DB state.
        """
        # Combine last DB state with new data
        combined = pd.concat([df_db_state, df_new], ignore_index=True)
        
        # Sort by group and date
        combined = combined.sort_values(by=group_cols + ["fecha_registro"])
        
        # Filter rows where state changes
        # Group by ID -> Shift state -> Compare
        # We need to keep the row if it's the *first* time we see it in this batch (compared to DB) OR if it changes within the batch
        
        combined['prev_state'] = combined.groupby(group_cols)[state_col].shift()
        
        # Changes: where current state != prev state
        # Note: The first row of 'df_new' for a group will compare against 'df_db_state' (if exists)
        changes = combined[combined[state_col] != combined['prev_state']]
        
        # We only want to insert the rows that come from df_new
        # The rows from df_db_state were just for comparison context
        # So we filter based on index or by checking if the row is in df_new
        # A safer way is to filter by fecha_registro > max(db_fecha) but we might process same timestamp data?
        # Let's rely on the fact that we concat [db, new]. 
        # Rows from db are at the top. 
        # We can drop rows that are exactly equal to what we fetched from DB?
        # Or simply: we only want to persist records that are physically in `df_new`.
        
        # Let's filter to keep only rows present in df_new (by index or key)
        # But df_new might have multiple rows.
        
        # Simpler approach:
        # The `changes` DF includes the rows that triggered a change.
        # If a row came from `df_db_state`, it means the state changed *relative to valid start*. 
        # Wait, if row is from DB, shift() is NaN (start). It appears in `changes` because NaN != State? 
        # actually NaN != State is True.
        # But we don't want to re-insert the DB row.
        
        # Filter out rows that belong to the DB set
        # We can use dates. "changes" must be from the new batch.
        
        # Get min date of new batch
        min_new_date = df_new['fecha_registro'].min()
        new_changes = changes[changes['fecha_registro'] >= min_new_date].copy()
        
        return new_changes.drop(columns=['prev_state'])

    def detect_suprimidos(self, df_batch_snapshot: pd.DataFrame, 
                         table_name: str, group_cols: List[str], state_col: str) -> pd.DataFrame:
        """
        Detect items that are missing in the current snapshot but were active in DB.
        
        Args:
            df_batch_snapshot: The latest snapshot of active cases.
            table_name: The target history table.
        """
        # 1. Get currently currently active items from DB (last known state is not 'Finished'/'Suprimido')
        # Ideally, we query unique IDs where last state != 'Folio suprimido'
        
        cols = ", ".join(group_cols + [state_col, "fecha_registro"])
        distinct_on = ", ".join(group_cols)
        
        query = f"""
        SELECT DISTINCT ON ({distinct_on}) *
        FROM {table_name}
        ORDER BY {distinct_on}, fecha_registro DESC
        """
        current_db_state = pd.read_sql_query(query, self.engine)
        
        if current_db_state.empty:
            return pd.DataFrame()
            
        # Filter active ones
        active_db = current_db_state[current_db_state[state_col] != "Folio suprimido"]
        
        # 2. Identify missing
        # Create a set of keys
        # We merge active_db with df_batch_snapshot on ID. 
        # Those present in active_db but NOT in snapshot are "Suprimidos".
        
        merged = pd.merge(active_db[group_cols + [state_col]], 
                          df_batch_snapshot[group_cols].assign(present=1), 
                          on=group_cols, 
                          how='left')
        
        missing = merged[merged['present'].isnull()].copy()
        
        if missing.empty:
            return pd.DataFrame()
        
        # 3. Create "Suprimido" rows
        # Date should be the snapshot date (meaning: at this time, it was gone)
        snapshot_date = df_batch_snapshot['fecha_registro'].max()
        
        missing[state_col] = "Folio suprimido"
        missing['fecha_registro'] = snapshot_date
        
        # Remove 'present' column
        missing = missing.drop(columns=['present'])
        
        return missing

    def get_static_client_start_dates(self) -> pd.DataFrame:
        """
        Get the first registration date for each client to determine Legacy vs Current.
        """
        query = "SELECT cliente, MIN(fecha_registro) as fecha_primer_registro FROM rn_registro_casos_transi GROUP BY cliente"
        return pd.read_sql_query(query, self.engine)

    def update_dimensions_padre(self, df_new_folios: List[str]):
        """
        Update the dimension table for the specific folios affected by the new batch.
        """
        if not df_new_folios:
            return

        logger.info(f"Updating Dimensions Padre for {len(df_new_folios)} folios...")
        
        # 1. Fetch History for these folios (from the OPTIMIZED history table)
        placeholders = ",".join([f"'{f}'" for f in df_new_folios])
        try:
            query_history = f"SELECT * FROM dm_rn_fechas_folio_padre WHERE folio_padre IN ({placeholders})"
            df_history = pd.read_sql_query(query_history, self.engine)
            if df_history.empty:
                return
        except Exception as e:
            logger.error(f"Error reading history for padre: {e}")
            return

        # 2. Fetch Static Info (from raw table, latest state)
        try:
            query_static = f"""
            SELECT DISTINCT ON (cliente, folio_padre) 
                cliente, folio_padre, fecha_asignacion, rut_paciente, nombre_paciente, intervencion_sanitaria, url_ficha 
            FROM rn_registro_casos_transi 
            WHERE folio_padre IN ({placeholders})
            ORDER BY cliente, folio_padre, fecha_registro DESC
            """
            df_static = pd.read_sql_query(query_static, self.engine)
            if df_static.empty:
                return 
            
            df_static = df_static[~df_static["url_ficha"].str.contains("codificado", na=False)]
            df_static["fecha_asignacion"] = pd.to_datetime(df_static["fecha_asignacion"])
        except Exception as e:
            logger.error(f"Error reading static info: {e}")
            return

        # 3. Calculate "Acceptance Date" / Logic (Replica of fix_ETL)
        # Need client start dates
        client_starts = self.get_static_client_start_dates()
        
        # Merge start dates
        df_proc = pd.merge(df_static, client_starts, on="cliente", how="left")
        
        # Split Current vs Legacy
        df_current = df_proc[df_proc["fecha_asignacion"] >= df_proc["fecha_primer_registro"]].copy()
        df_legacy = df_proc[df_proc["fecha_asignacion"] < df_proc["fecha_primer_registro"]].copy()
        
        # --- Logic for Current ---
        if not df_current.empty:
            df_current["tipo_seguimiento"] = "current"
            hist_curr = df_history[df_history["folio_padre"].isin(df_current["folio_padre"])]
            
            non_accepted_states = ["Asignado", "Folio suprimido", "SIN Prestador", "Rechazo paciente sin consulta", "No acepta tratamiento"]
            valid_states = hist_curr[~hist_curr["estado_padre"].isin(non_accepted_states)]
            
            if not valid_states.empty:
                first_valid = valid_states.sort_values("fecha_registro").groupby(["cliente", "folio_padre"]).first().reset_index()
                df_current = pd.merge(df_current, first_valid[["cliente", "folio_padre", "fecha_registro"]], on=["cliente", "folio_padre"], how="left")
                df_current.rename(columns={"fecha_registro": "fecha_asignacion_aceptada"}, inplace=True)
            else:
                 df_current["fecha_asignacion_aceptada"] = None
        
        # --- Logic for Legacy ---
        if not df_legacy.empty:
            df_legacy["tipo_seguimiento"] = "legacy"
            hist_leg = df_history[df_history["folio_padre"].isin(df_legacy["folio_padre"])]
            
            has_asignado = hist_leg[hist_leg["estado_padre"] == "Asignado"]["folio_padre"].unique()
            
            if not hist_leg.empty:
                first_non_asignado = hist_leg[hist_leg["estado_padre"] != "Asignado"].sort_values("fecha_registro").groupby(["cliente", "folio_padre"]).first().reset_index()
                df_legacy = pd.merge(df_legacy, first_non_asignado[["cliente", "folio_padre", "fecha_registro"]], on=["cliente", "folio_padre"], how="left")
            else:
                df_legacy["fecha_registro"] = None

            # Apply condition
            mask_identifiable = df_legacy["folio_padre"].isin(has_asignado)
            # handle NaN dates carefully
            df_legacy["fecha_asignacion_aceptada"] = np.where(mask_identifiable, df_legacy["fecha_registro"], df_legacy["fecha_asignacion"])
            df_legacy.drop(columns=["fecha_registro"], inplace=True, errors='ignore')

        # Combine
        df_final = pd.concat([df_current, df_legacy], ignore_index=True)
        
        # 4. Add Extra info (Details)
        if not df_final.empty:
            urls = df_final["url_ficha"].unique()
            if len(urls) > 0:
                urls_str = ",".join([f"'{u}'" for u in urls])
                try:
                    # Using limit/distinct on logic
                    query_details = f"""
                    SELECT DISTINCT ON (url_ficha) url_ficha, fecha_atencion, intervencion_sanitaria_ingreso, problema_salud 
                    FROM rn_detalle_casos_info_general 
                    WHERE url_ficha IN ({urls_str})
                    """
                    # Use a robust way to deduplicate if no explicit order column (or assume random is OK for static info)
                    df_details = pd.read_sql_query(query_details, self.engine)
                    # Deduplicate in pandas just in case SQL didn't
                    df_details = df_details.drop_duplicates(subset=["url_ficha"], keep="last")
                    
                    df_final = pd.merge(df_final, df_details, on="url_ficha", how="left")
                except Exception as e:
                    logger.warning(f"Could not load info general: {e}")

            # 5. Add Latest State (from History)
            last_state = df_history.sort_values("fecha_registro").groupby(["cliente", "folio_padre"]).last().reset_index()
            last_state = last_state[["cliente", "folio_padre", "estado_padre", "fecha_registro"]]
            last_state.rename(columns={"estado_padre": "ultimo_estado_folio_padre", "fecha_registro": "fecha_ultimo_estado_folio_padre"}, inplace=True)
            
            df_final = pd.merge(df_final, last_state, on=["cliente", "folio_padre"], how="left")
            
            # 6. UPSERT into Postgres
            # Delete existing rows
            try:
                with self.engine.connect() as conn:
                    # chunk deletion if many?
                    conn.execute(text(f"DELETE FROM dm_rn_folio_padre WHERE folio_padre IN ({placeholders})"))
                    conn.commit()
            except Exception as e:
                logger.error(f"Error during delete: {e}")

            # Insert new
            cols_to_drop = ["fecha_primer_registro"]
            df_final.drop(columns=[c for c in cols_to_drop if c in df_final.columns], inplace=True)
            
            logger.info(f"Upserting {len(df_final)} rows to dm_rn_folio_padre")
            df_final.to_sql("dm_rn_folio_padre", self.engine, if_exists="append", index=False)

    def update_dimensions_hijo(self, df_new_hijos: pd.DataFrame):
        """
        Update dimension table for folio hijo. 
        df_new_hijos contains columns: cliente, folio_padre, folio_hijo
        """
        if df_new_hijos.empty:
            return

        logger.info(f"Updating Dimensions Hijo for {len(df_new_hijos)} records...")
        
        # Get unique keys
        keys = df_new_hijos[["cliente", "folio_padre", "folio_hijo"]].drop_duplicates()
        if keys.empty:
            return

        # 1. Fetch Static/Base Info
        placeholders_hijo = ",".join([f"'{h}'" for h in keys["folio_hijo"]])
        
        try:
            query_base = f"""
            SELECT DISTINCT ON (cliente, folio_padre, folio_hijo) 
                cliente, folio_padre, folio_hijo, intervencion_sanitaria, ppa_grd, monto_total, url_ficha 
            FROM rn_registro_casos_transi 
            WHERE folio_hijo IN ({placeholders_hijo})
            ORDER BY cliente, folio_padre, folio_hijo, fecha_registro DESC
            """
            df_base = pd.read_sql_query(query_base, self.engine)
        except Exception as e:
            logger.error(f"Error getting base hijo info: {e}")
            return

        # 2. Add Prestaciones Details
        try:
             query_prest_simple = f"SELECT * FROM rn_detalle_casos_prestaciones WHERE folio IN ({placeholders_hijo})"
             df_prest = pd.read_sql_query(query_prest_simple, self.engine)
             if not df_prest.empty:
                 df_prest = df_prest.drop_duplicates(subset=["folio"], keep="last")
                 # Check cols
                 if "estado" in df_prest.columns:
                    df_prest.rename(columns={"estado": "estado_prestacion"}, inplace=True)
                 
                 df_base = pd.merge(df_base, df_prest[["cliente", "folio", "descripcion", "estado_prestacion"]], left_on=["cliente", "folio_hijo"], right_on=["cliente", "folio"], how="left")
                 if "folio" in df_base.columns: df_base.drop(columns=["folio"], inplace=True)
        except Exception as e:
            logger.warning(f"Could not load prestaciones details: {e}")

        # 3. Add Tipo Seguimiento (from Padre Dimension)
        if not df_base.empty:
            folio_padres = keys["folio_padre"].unique()
            if len(folio_padres) > 0:
                placeholders_padre = ",".join([f"'{p}'" for p in folio_padres])
                try:
                    query_padre = f"SELECT cliente, folio_padre, tipo_seguimiento FROM dm_rn_folio_padre WHERE folio_padre IN ({placeholders_padre})"
                    df_padre_info = pd.read_sql_query(query_padre, self.engine)
                    df_base = pd.merge(df_base, df_padre_info, on=["cliente", "folio_padre"], how="left")
                except Exception:
                    pass

        # 4. Add First/Last States from History (Hijo)
        try:
            query_hist_hijo = f"SELECT * FROM dm_rn_fechas_folio_hijo WHERE folio_hijo IN ({placeholders_hijo})"
            df_hist_hijo = pd.read_sql_query(query_hist_hijo, self.engine)
            
            if not df_hist_hijo.empty:
                # Last State
                last = df_hist_hijo.sort_values("fecha_registro").groupby(["cliente", "folio_padre", "folio_hijo"]).last().reset_index()
                last = last[["cliente", "folio_padre", "folio_hijo", "estado_hijo", "fecha_registro"]]
                last.rename(columns={"estado_hijo": "ultimo_estado_folio_hijo", "fecha_registro": "fecha_ultimo_estado_folio_hijo"}, inplace=True)
                
                # First State
                first = df_hist_hijo.sort_values("fecha_registro").groupby(["cliente", "folio_padre", "folio_hijo"]).first().reset_index()
                first = first[["cliente", "folio_padre", "folio_hijo", "estado_hijo", "fecha_registro"]]
                first.rename(columns={"estado_hijo": "primer_estado_folio_hijo", "fecha_registro": "fecha_primer_estado_folio_hijo"}, inplace=True)
                
                df_base = pd.merge(df_base, last, on=["cliente", "folio_padre", "folio_hijo"], how="left")
                df_base = pd.merge(df_base, first, on=["cliente", "folio_padre", "folio_hijo"], how="left")
        except Exception as e:
            logger.error(f"Error reading hijo history: {e}")

        # 5. Format GRD
        if "ppa_grd" in df_base.columns and "descripcion" in df_base.columns:
            df_base["grd"] = np.where(df_base["ppa_grd"] == "GRD", 
                                      df_base["descripcion"].str.split(":").str[1].str.strip().str.zfill(6), 
                                      None)

        # 6. UPSERT
        try:
            with self.engine.connect() as conn:
                conn.execute(text(f"DELETE FROM dm_rn_folio_hijo WHERE folio_hijo IN ({placeholders_hijo})"))
                conn.commit()
                
            logger.info(f"Upserting {len(df_base)} rows to dm_rn_folio_hijo")
            df_base.to_sql("dm_rn_folio_hijo", self.engine, if_exists="append", index=False)
        except Exception as e:
            logger.error(f"Error during upsert hijo: {e}")

    def run(self):
        try:
            logger.info("Starting Incremental ETL Process...")
            
            # 1. Determine Watermark
            # We use dm_rn_fechas_folio_padre as the main tracker for "raw" updates history
            max_date = self.get_max_date("dm_rn_fechas_folio_padre")
            
            # 2. Extract Data
            df_new = self.extract_new_data(max_date)
            
            if df_new.empty:
                logger.info("No new data found. Exiting.")
                return

            # Ensure data types
            if 'fecha_registro' in df_new.columns:
                df_new['fecha_registro'] = pd.to_datetime(df_new['fecha_registro'])
            
            # 3. Process Folio Padre
            logger.info("Processing Folio Padre...")
            df_state_padre = self.get_latest_state_from_db("dm_rn_fechas_folio_padre", ['cliente', 'folio_padre'], 'estado_padre')
            
            padre_cols = ["cliente", "folio_padre", "fecha_registro", "estado_padre"]
            if all(col in df_new.columns for col in padre_cols):
                df_changes_padre = self.process_status_changes(df_new[padre_cols], df_state_padre, ['cliente', 'folio_padre'], 'estado_padre')
                
                # Detect Suprimidos
                latest_ts = df_new['fecha_registro'].max()
                latest_snapshot = df_new[df_new['fecha_registro'] == latest_ts]
                
                df_suprimidos_padre = self.detect_suprimidos(latest_snapshot, "dm_rn_fechas_folio_padre", ['cliente', 'folio_padre'], 'estado_padre')
                
                if not df_suprimidos_padre.empty:
                    df_changes_padre = pd.concat([df_changes_padre, df_suprimidos_padre], ignore_index=True)
                
                # Persist Padre History
                if not df_changes_padre.empty:
                    logger.info(f"Appending {len(df_changes_padre)} rows to dm_rn_fechas_folio_padre")
                    df_changes_padre.to_sql("dm_rn_fechas_folio_padre", self.engine, if_exists="append", index=False)
                    
                    # Update Dimensions
                    affected_folios = df_new["folio_padre"].unique().tolist()
                    if not df_suprimidos_padre.empty:
                        affected_folios.extend(df_suprimidos_padre["folio_padre"].unique().tolist())
                    
                    self.update_dimensions_padre(list(set(affected_folios)))
            else:
                logger.warning("Missing required columns for processing Folio Padre.")
            
            # 4. Process Folio Hijo
            logger.info("Processing Folio Hijo...")
            hijo_cols = ["cliente", "folio_padre", "folio_hijo", "ppa_grd", "fecha_registro", "estado_padre", "estado_hijo"]
            
            # Check availability
            available_hijo_cols = [c for c in hijo_cols if c in df_new.columns]
            if len(available_hijo_cols) == len(hijo_cols):
                df_hijo_raw = df_new[hijo_cols].copy()
                df_hijo_raw = df_hijo_raw[~df_hijo_raw["folio_hijo"].isin(["", None])]
                
                if not df_hijo_raw.empty:
                    df_state_hijo = self.get_latest_state_from_db("dm_rn_fechas_folio_hijo", ['cliente', 'folio_hijo'], 'estado_hijo')
                    df_changes_hijo = self.process_status_changes(df_hijo_raw, df_state_hijo, ['cliente', 'folio_hijo'], 'estado_hijo')
                    
                    # Suprimidos Hijo
                    latest_snapshot_hijo = latest_snapshot[~latest_snapshot["folio_hijo"].isin(["", None])]
                    df_suprimidos_hijo = self.detect_suprimidos(latest_snapshot_hijo, "dm_rn_fechas_folio_hijo", ['cliente', 'folio_hijo'], 'estado_hijo')
                    
                    if not df_suprimidos_hijo.empty:
                        full_state = self.get_latest_state_from_db("dm_rn_fechas_folio_hijo", ['cliente', 'folio_hijo'], 'estado_hijo')
                        if not full_state.empty:
                            df_suprimidos_hijo_full = pd.merge(
                                df_suprimidos_hijo[['cliente', 'folio_hijo', 'estado_hijo', 'fecha_registro']], 
                                full_state[['cliente', 'folio_hijo', 'folio_padre', 'ppa_grd', 'estado_padre']], 
                                on=['cliente', 'folio_hijo'], 
                                how='left'
                            )
                            df_changes_hijo = pd.concat([df_changes_hijo, df_suprimidos_hijo_full], ignore_index=True)

                    if not df_changes_hijo.empty:
                        logger.info(f"Appending {len(df_changes_hijo)} rows to dm_rn_fechas_folio_hijo")
                        df_changes_hijo.to_sql("dm_rn_fechas_folio_hijo", self.engine, if_exists="append", index=False)
                        
                        # Update Dimensions Hijo
                        affected_hijos_df = pd.concat([df_hijo_raw, df_suprimidos_hijo], ignore_index=True) if not df_suprimidos_hijo.empty else df_hijo_raw
                        self.update_dimensions_hijo(affected_hijos_df)
            else:
                logger.warning("Missing required columns for processing Folio Hijo.")

            logger.info("ETL Run Complete Successfully.")
            
        except Exception as e:
            logger.critical(f"ETL failed in run loop: {e}")
            raise

if __name__ == "__main__":
    etl = IncrementalETL(settings.POSTGRES_URI)
    etl.run()
