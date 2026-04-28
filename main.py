from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import psycopg2.extras
import json

app = FastAPI(title="Feminicidios BDE API — Sector El Playon")

app.add_middleware(CORSMiddleware,allow_origins=["*"],allow_methods=["*"],allow_headers=["*"])

DB_CONFIG = {"dbname":"Feminicidios_modelo ","user":"postgres","password":"Andresj1204","host":"localhost","port":5432}

def get_conn():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.set_client_encoding("UTF8")
    return conn

MODULES = {
    "EA":{"label":"Evento, Lugar y Arma","color":"#FF3860","icon":"⚠️"},
    "EM":{"label":"Equipamiento y Movilidad","color":"#00C9D4","icon":"🏛️"},
    "SC":{"label":"Seguridad y Convivencia","color":"#FFCA3A","icon":"🛡️"},
    "TE":{"label":"Territorial y Catastral","color":"#6366F1","icon":"🗺️"},
    "TV":{"label":"Trayectoria Vital","color":"#9B6DFF","icon":"👤"},
}

LAYERS = {
    "ea_lugar_hecho":{"label":"Lugares del Hecho","table":"ea_lugar_hecho","geom":"geom","color":"#FF3860","radius":35,"blur":25,"intensity":1.0,"filter":None,"tot_table":"tot_feminicidios_territoriales","icon":"🔴","type":"point","module":"EA"},
    "ea_lugar_abandono_cuerpo":{"label":"Abandono del Cuerpo","table":"ea_lugar_abandono_cuerpo","geom":"geom","color":"#6B21A8","radius":35,"blur":25,"intensity":1.0,"filter":None,"tot_table":None,"icon":"☠️","type":"point","module":"EA"},
    "ea_vendedor_proveedor_arma":{"label":"Vendedores / Proveedores Arma","table":"ea_vendedor_proveedor_arma","geom":"geom","color":"#B91C1C","radius":28,"blur":20,"intensity":0.9,"filter":None,"tot_table":None,"icon":"🔫","type":"point","module":"EA"},
    "em_equipamiento_urbano":{"label":"Equipamiento Urbano","table":"em_equipamiento_urbano","geom":"geom","color":"#00C9D4","radius":28,"blur":20,"intensity":0.6,"filter":"activo = true","tot_table":None,"icon":"🏛️","type":"point","module":"EM"},
    "em_proximidad_equipamiento":{"label":"Proximidad Equipamiento","table":"em_proximidad_equipamiento","geom":"geom_ruta","color":"#22D3EE","radius":None,"blur":None,"intensity":None,"filter":"geom_ruta IS NOT NULL","tot_table":None,"icon":"📐","type":"line","module":"EM"},
    "em_ruta_movilidad_v":{"label":"Rutas Movilidad — Víctima","table":"em_ruta_movilidad","geom":"geom","color":"#FCD34D","radius":None,"blur":None,"intensity":None,"filter":"id_victima IS NOT NULL AND geom IS NOT NULL","tot_table":None,"icon":"🛣️","type":"line","module":"EM","role":"victima"},
    "em_ruta_movilidad_a":{"label":"Rutas Movilidad — Atacante","table":"em_ruta_movilidad","geom":"geom","color":"#F97316","radius":None,"blur":None,"intensity":None,"filter":"id_atacante IS NOT NULL AND geom IS NOT NULL","tot_table":None,"icon":"🛣️","type":"line","module":"EM","role":"atacante"},
    "red_vial":{"label":"Red Vial","table":"red_vial","geom":"geom","color":"#94A3B8","radius":None,"blur":None,"intensity":None,"filter":None,"tot_table":None,"icon":"🛤️","type":"line","module":"EM"},
    "sc_incidente_convivencia":{"label":"Incidentes Convivencia","table":"sc_incidente_convivencia","geom":"geom","color":"#FFCA3A","radius":25,"blur":18,"intensity":0.7,"filter":None,"tot_table":"tot_incidentes_periodo","icon":"🟡","type":"point","module":"SC"},
    "sc_punto_microtrafico":{"label":"Puntos Microtrafico","table":"sc_punto_microtrafico","geom":"geom","color":"#FF6B35","radius":30,"blur":20,"intensity":0.8,"filter":"estado = 'activo'","tot_table":"tot_microtrafico_zona","icon":"🟠","type":"point","module":"SC"},
    "te_area_actividad":{"label":"Area de Actividad","table":"te_area_actividad","geom":"geom","color":"#F59E0B","radius":None,"blur":None,"intensity":None,"filter":None,"tot_table":None,"icon":"🟨","type":"polygon","module":"TE","color_field":"tipo_actividad"},
    "te_clase_suelo":{"label":"Clase de Suelo","table":"te_clase_suelo","geom":"geom","color":"#10B981","radius":None,"blur":None,"intensity":None,"filter":None,"tot_table":None,"icon":"🟩","type":"polygon","module":"TE","color_field":"clasificacion_pot"},
    "te_division_administrativa":{"label":"Division Administrativa","table":"te_division_administrativa","geom":"geom","color":"#6366F1","radius":None,"blur":None,"intensity":None,"filter":None,"tot_table":None,"icon":"🟦","type":"polygon","module":"TE","color_field":"nivel"},
    "te_influencia_vial":{"label":"Influencia Vial","table":"te_influencia_vial","geom":"geom","color":"#EC4899","radius":None,"blur":None,"intensity":None,"filter":None,"tot_table":None,"icon":"🔷","type":"polygon","module":"TE","color_field":"tipo_via"},
    "te_servicios_publicos":{"label":"Servicios Publicos","table":"te_servicios_publicos","geom":"geom","color":"#14B8A6","radius":None,"blur":None,"intensity":None,"filter":None,"tot_table":None,"icon":"🔹","type":"polygon","module":"TE","color_field":"tipo_servicio"},
    "te_territorio":{"label":"Territorio (Limite)","table":"te_territorio","geom":"geom","color":"#FF3860","radius":None,"blur":None,"intensity":None,"filter":"id_territorio = 1","tot_table":None,"icon":"🗺️","type":"polygon","module":"TE"},
    "te_tipo_predio":{"label":"Tipo de Predio","table":"te_tipo_predio","geom":"geom","color":"#F97316","radius":None,"blur":None,"intensity":None,"filter":None,"tot_table":None,"icon":"🏠","type":"polygon","module":"TE","color_field":"tipo_predio"},
    "te_topografia":{"label":"Topografia","table":"te_topografia","geom":"geom","color":"#84CC16","radius":None,"blur":None,"intensity":None,"filter":None,"tot_table":None,"icon":"⛰️","type":"polygon","module":"TE","color_field":"tipo_relieve"},
    "te_tratamiento":{"label":"Tratamiento Urbanistico","table":"te_tratamiento","geom":"geom","color":"#A855F7","radius":None,"blur":None,"intensity":None,"filter":None,"tot_table":None,"icon":"🔮","type":"polygon","module":"TE","color_field":"tipo"},
    "te_uso_suelo":{"label":"Uso de Suelo","table":"te_uso_suelo","geom":"geom","color":"#06B6D4","radius":None,"blur":None,"intensity":None,"filter":None,"tot_table":None,"icon":"🗾","type":"polygon","module":"TE","color_field":"categoria"},
    "tv_lugar_asilo_refugio":{"label":"Asilo / Refugio — Víctima","table":"tv_lugar_asilo_refugio","geom":"geom","color":"#0EA5E9","radius":25,"blur":18,"intensity":0.7,"filter":"id_victima IS NOT NULL","tot_table":None,"icon":"🏠","type":"point","module":"TV","role":"victima"},
    "tv_lugar_crianza_v":{"label":"Crianza — Víctima","table":"tv_lugar_crianza","geom":"geom","color":"#22C55E","radius":28,"blur":20,"intensity":0.7,"filter":"id_victima IS NOT NULL","tot_table":None,"icon":"🟢","type":"point","module":"TV","role":"victima"},
    "tv_lugar_crianza_a":{"label":"Crianza — Atacante","table":"tv_lugar_crianza","geom":"geom","color":"#DC2626","radius":28,"blur":20,"intensity":0.7,"filter":"id_atacante IS NOT NULL","tot_table":None,"icon":"🔴","type":"point","module":"TV","role":"atacante"},
    "tv_lugar_estudio_v":{"label":"Estudio — Víctima","table":"tv_lugar_estudio","geom":"geom","color":"#3B82F6","radius":25,"blur":18,"intensity":0.7,"filter":"id_victima IS NOT NULL","tot_table":None,"icon":"📚","type":"point","module":"TV","role":"victima"},
    "tv_lugar_estudio_a":{"label":"Estudio — Atacante","table":"tv_lugar_estudio","geom":"geom","color":"#EA580C","radius":25,"blur":18,"intensity":0.7,"filter":"id_atacante IS NOT NULL","tot_table":None,"icon":"📕","type":"point","module":"TV","role":"atacante"},
    "tv_lugar_recreacion_v":{"label":"Recreación — Víctima","table":"tv_lugar_recreacion","geom":"geom","color":"#F472B6","radius":25,"blur":18,"intensity":0.7,"filter":"id_victima IS NOT NULL","tot_table":None,"icon":"⚽","type":"point","module":"TV","role":"victima"},
    "tv_lugar_recreacion_a":{"label":"Recreación — Atacante","table":"tv_lugar_recreacion","geom":"geom","color":"#B91C1C","radius":25,"blur":18,"intensity":0.7,"filter":"id_atacante IS NOT NULL","tot_table":None,"icon":"🔺","type":"point","module":"TV","role":"atacante"},
    "tv_lugar_residencia_v":{"label":"Residencia — Víctima","table":"tv_lugar_residencia","geom":"geom","color":"#9B6DFF","radius":30,"blur":22,"intensity":0.9,"filter":"id_victima IS NOT NULL","tot_table":None,"icon":"🟣","type":"point","module":"TV","role":"victima"},
    "tv_lugar_residencia_a":{"label":"Residencia — Atacante","table":"tv_lugar_residencia","geom":"geom","color":"#9F1239","radius":30,"blur":22,"intensity":0.9,"filter":"id_atacante IS NOT NULL","tot_table":None,"icon":"🔴","type":"point","module":"TV","role":"atacante"},
    "tv_lugar_trabajo_v":{"label":"Trabajo — Víctima","table":"tv_lugar_trabajo","geom":"geom","color":"#FB923C","radius":25,"blur":18,"intensity":0.7,"filter":"id_victima IS NOT NULL","tot_table":None,"icon":"💼","type":"point","module":"TV","role":"victima"},
    "tv_lugar_trabajo_a":{"label":"Trabajo — Atacante","table":"tv_lugar_trabajo","geom":"geom","color":"#991B1B","radius":25,"blur":18,"intensity":0.7,"filter":"id_atacante IS NOT NULL","tot_table":None,"icon":"🗡️","type":"point","module":"TV","role":"atacante"},
}


@app.get("/layers")
def get_layers():
    return {
        "modules":[{"id":k,"label":v["label"],"color":v["color"],"icon":v["icon"]} for k,v in MODULES.items()],
        "layers":[{"id":k,"label":v["label"],"color":v["color"],"icon":v["icon"],"has_stats":v["tot_table"] is not None,"type":v.get("type","point"),"module":v.get("module","EA"),"color_field":v.get("color_field"),"role":v.get("role")} for k,v in LAYERS.items()],
    }

@app.get("/territory/geojson")
def get_territory():
    conn=get_conn();cur=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT ST_AsGeoJSON(ST_Transform(geom,4326)) AS g FROM te_territorio WHERE id_territorio=1 LIMIT 1")
    row=cur.fetchone();cur.close();conn.close()
    if not row:raise HTTPException(404,"Territorio no encontrado")
    return {"geojson":json.loads(row["g"])}

@app.get("/territory/mask")
def get_territory_mask():
    conn=get_conn();cur=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT ST_AsGeoJSON(ST_Transform(ST_Difference(ST_MakeEnvelope(-74.20,4.53,-74.08,4.60,4326),ST_Union(geom)),4326)) AS g FROM te_territorio WHERE id_territorio=1")
    row=cur.fetchone();cur.close();conn.close()
    if not row or not row["g"]:raise HTTPException(500,"Mascara no generada")
    return {"geojson":json.loads(row["g"])}

@app.get("/layer/{layer_id}/points")
def get_layer_points(layer_id:str):
    if layer_id not in LAYERS:raise HTTPException(404,"Capa no encontrada")
    cfg=LAYERS[layer_id]
    if cfg.get("type") not in ("point",None):raise HTTPException(400,"Usa /geojson para esta capa")
    conn=get_conn();cur=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cond=f"WHERE {cfg['filter']}" if cfg.get("filter") else "WHERE TRUE"
    cur.execute(f"SELECT ST_Y(ST_Transform({cfg['geom']},4326)) AS lat,ST_X(ST_Transform({cfg['geom']},4326)) AS lon FROM {cfg['table']} {cond} AND {cfg['geom']} IS NOT NULL")
    rows=cur.fetchall();cur.close();conn.close()
    return {"layer":cfg["label"],"color":cfg["color"],"radius":cfg["radius"],"blur":cfg["blur"],"intensity":cfg["intensity"],"count":len(rows),"points":[[float(r["lat"]),float(r["lon"])] for r in rows]}

@app.get("/layer/{layer_id}/geojson")
def get_layer_geojson(layer_id: str):
    if layer_id not in LAYERS: raise HTTPException(404, "Capa no encontrada")
    cfg   = LAYERS[layer_id]
    ltype = cfg.get("type", "polygon")
    if ltype not in ("polygon", "line"): raise HTTPException(400, "Usa /points para esta capa")
    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cond  = f"WHERE {cfg['filter']}" if cfg.get("filter") else ""
    cf    = cfg.get("color_field")
    geom  = cfg["geom"]
    # Propiedades: campo categorico + metrica geometrica via PostGIS
    parts = []
    if cf:
        parts.append(f"'{cf}', {cf}")
    if ltype == "polygon":
        parts.append(f"'_area_m2', ROUND(ST_Area({geom}::geography)::numeric, 2)")
    elif ltype == "line":
        parts.append(f"'_length_m', ROUND(ST_Length({geom}::geography)::numeric, 2)")
    ps  = f"json_build_object({', '.join(parts)})" if parts else "'{}'::json"
    sep = "AND" if cond else "WHERE"
    try:
        q = f"""SELECT json_build_object(
                'type', 'FeatureCollection',
                'features', COALESCE(json_agg(json_build_object(
                    'type', 'Feature',
                    'geometry', ST_AsGeoJSON(ST_Transform({geom}, 4326))::json,
                    'properties', {ps}
                )), '[]'::json)
            ) AS g FROM {cfg['table']} {cond} {sep} {geom} IS NOT NULL"""
        cur.execute(q)
        row = cur.fetchone()
    except Exception as e:
        cur.close(); conn.close(); raise HTTPException(500, str(e))
    cur.close(); conn.close()
    if not row or not row["g"]: raise HTTPException(404, "Sin datos")
    gj = row["g"]
    if isinstance(gj, str): gj = json.loads(gj)
    return {"layer": layer_id, "label": cfg["label"], "color": cfg["color"],
            "type": ltype, "color_field": cf, "geojson": gj}

@app.get("/layer/{layer_id}/stats")
def get_layer_stats(layer_id:str):
    if layer_id not in LAYERS:raise HTTPException(404,"Capa no encontrada")
    cfg=LAYERS[layer_id]
    if not cfg["tot_table"]:return {"stats":None}
    conn=get_conn();cur=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    if cfg["tot_table"]=="tot_feminicidios_territoriales":
        cur.execute("SELECT SUM(total_feminicidios) AS fem,SUM(total_tentativas) AS tent,SUM(total_con_sentencia) AS sent,SUM(total_impunes) AS imp,ROUND(AVG(tasa_por_100k_mujeres)::numeric,2) AS tasa,modalidad_mas_frecuente AS mod,hora_mas_frecuente AS hora,dia_semana_mas_frecuente AS dia FROM tot_feminicidios_territoriales GROUP BY modalidad_mas_frecuente,hora_mas_frecuente,dia_semana_mas_frecuente LIMIT 1")
        r=cur.fetchone()
        cur.execute("SELECT tipo_arma,COUNT(*) AS n FROM ea_arma_utilizada GROUP BY tipo_arma ORDER BY n DESC LIMIT 6")
        armas=cur.fetchall()
        cur.execute("SELECT modalidad,COUNT(*) AS n FROM fm_feminicidio GROUP BY modalidad ORDER BY n DESC")
        mods=cur.fetchall()
        cur.execute("SELECT estado_caso,COUNT(*) AS n FROM fm_feminicidio GROUP BY estado_caso ORDER BY n DESC")
        estados=cur.fetchall()
        cur.execute("SELECT estado_captura_atacante AS estado,COUNT(*) AS n FROM fm_feminicidio GROUP BY estado_captura_atacante ORDER BY n DESC")
        capturas=cur.fetchall()
        cur.close();conn.close()
        if not r:return {"stats":None}
        return {"stats":{
            "title":"Feminicidios — Sector El Playon 2024","tot_table":"tot_feminicidios_territoriales",
            "cards":[
                {"label":"Consumados","value":int(r["fem"] or 0),"color":"#FF3860"},
                {"label":"Tentativas","value":int(r["tent"] or 0),"color":"#FF9500"},
                {"label":"Con sentencia","value":int(r["sent"] or 0),"color":"#10B981"},
                {"label":"Impunes/archivados","value":int(r["imp"] or 0),"color":"#6B7280"},
                {"label":"Tasa x 100k mujeres","value":str(r["tasa"] or "0"),"color":"#8B5CF6"},
                {"label":"Modalidades","value":len(mods),"color":"#00C9D4"},
            ],
            "pills":[
                {"label":"Modalidad frecuente","value":str(r["mod"] or "N/A")},
                {"label":"Hora frecuente","value":str(r["hora"] or "N/A")},
                {"label":"Dia frecuente","value":str(r["dia"] or "N/A")},
            ],
            "charts":[
                {"label":"Armas utilizadas","data":[{"label":a["tipo_arma"],"value":int(a["n"])} for a in armas],"color":"#FF3860"},
                {"label":"Por modalidad","data":[{"label":m["modalidad"],"value":int(m["n"])} for m in mods],"color":"#FF9500"},
                {"label":"Estado del caso","data":[{"label":e["estado_caso"],"value":int(e["n"])} for e in estados],"color":"#10B981"},
                {"label":"Estado captura","data":[{"label":c["estado"],"value":int(c["n"])} for c in capturas],"color":"#8B5CF6"},
            ],
        }}

    elif cfg["tot_table"]=="tot_microtrafico_zona":
        cur.execute("SELECT SUM(total_puntos_activos) AS act,SUM(total_puntos_desmantelados) AS des,ROUND(AVG(radio_influencia_promedio_m)::numeric,1) AS radio,SUM(puntos_cerca_instituciones_edu) AS edu,SUM(casos_fem_con_microtrafico_zona) AS fem FROM tot_microtrafico_zona")
        r=cur.fetchone()
        cur.execute("SELECT COALESCE(tipo_sustancia,'desconocido') AS sust,SUM(total_puntos_activos) AS n FROM tot_microtrafico_zona GROUP BY tipo_sustancia ORDER BY n DESC")
        sust=cur.fetchall()
        cur.execute("SELECT tipo_punto,COUNT(*) AS n FROM sc_punto_microtrafico GROUP BY tipo_punto ORDER BY n DESC")
        tipos=cur.fetchall()
        cur.execute("SELECT frecuencia_actividad,COUNT(*) AS n FROM sc_punto_microtrafico GROUP BY frecuencia_actividad ORDER BY n DESC")
        frec=cur.fetchall()
        cur.close();conn.close()
        if not r:return {"stats":None}
        return {"stats":{
            "title":"Microtrafico — Sector El Playon 2024","tot_table":"tot_microtrafico_zona",
            "cards":[
                {"label":"Puntos activos","value":int(r["act"] or 0),"color":"#FF6B35"},
                {"label":"Desmantelados","value":int(r["des"] or 0),"color":"#10B981"},
                {"label":"Radio influencia (m)","value":str(r["radio"] or "0"),"color":"#FFCA3A"},
                {"label":"Cerca de colegios","value":int(r["edu"] or 0),"color":"#FF3860"},
                {"label":"Casos fem asociados","value":int(r["fem"] or 0),"color":"#8B5CF6"},
                {"label":"Tipos sustancia","value":len(sust),"color":"#00C9D4"},
            ],
            "pills":[],
            "charts":[
                {"label":"Por sustancia","data":[{"label":s["sust"],"value":int(s["n"] or 0)} for s in sust],"color":"#FF6B35"},
                {"label":"Por tipo de punto","data":[{"label":t["tipo_punto"],"value":int(t["n"])} for t in tipos],"color":"#FFCA3A"},
                {"label":"Por frecuencia","data":[{"label":f["frecuencia_actividad"],"value":int(f["n"])} for f in frec],"color":"#10B981"},
            ],
        }}

    elif cfg["tot_table"]=="tot_incidentes_periodo":
        cur.execute("SELECT SUM(total_incidentes) AS total,SUM(total_con_intervencion_policial) AS polic,SUM(total_que_escalaron_denuncia) AS escal,ROUND(AVG(tiempo_respuesta_promedio_min)::numeric,1) AS tresp FROM tot_incidentes_periodo")
        r=cur.fetchone()
        cur.execute("SELECT tipo_incidente,COUNT(*) AS n FROM sc_incidente_convivencia GROUP BY tipo_incidente ORDER BY n DESC")
        tipos=cur.fetchall()
        cur.execute("SELECT dia_semana,COUNT(*) AS n FROM sc_incidente_convivencia GROUP BY dia_semana ORDER BY n DESC")
        dias=cur.fetchall()
        cur.execute("SELECT subtipo,COUNT(*) AS n FROM sc_incidente_convivencia GROUP BY subtipo ORDER BY n DESC LIMIT 6")
        subs=cur.fetchall()
        cur.close();conn.close()
        if not r:return {"stats":None}
        return {"stats":{
            "title":"Incidentes de Convivencia — El Playon 2024","tot_table":"tot_incidentes_periodo",
            "cards":[
                {"label":"Total incidentes","value":int(r["total"] or 0),"color":"#FFCA3A"},
                {"label":"Con policia","value":int(r["polic"] or 0),"color":"#00C9D4"},
                {"label":"Escalaron a denuncia","value":int(r["escal"] or 0),"color":"#FF3860"},
                {"label":"T. respuesta (min)","value":str(r["tresp"] or "0"),"color":"#8B5CF6"},
                {"label":"Tipos registrados","value":len(tipos),"color":"#10B981"},
                {"label":"Dias con incidentes","value":len(dias),"color":"#FF6B35"},
            ],
            "pills":[
                {"label":"Tipo frecuente","value":tipos[0]["tipo_incidente"] if tipos else "N/A"},
                {"label":"Dia frecuente","value":dias[0]["dia_semana"] if dias else "N/A"},
            ],
            "charts":[
                {"label":"Por tipo","data":[{"label":t["tipo_incidente"],"value":int(t["n"])} for t in tipos],"color":"#FFCA3A"},
                {"label":"Por dia","data":[{"label":d["dia_semana"],"value":int(d["n"])} for d in dias],"color":"#00C9D4"},
                {"label":"Por subtipo","data":[{"label":s["subtipo"],"value":int(s["n"])} for s in subs],"color":"#FF3860"},
            ],
        }}

    cur.close();conn.close()
    return {"stats":None}

@app.get("/overview")
def get_overview():
    conn=get_conn();cur=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""SELECT (SELECT COUNT(*) FROM fm_feminicidio) AS feminicidios,(SELECT COUNT(*) FROM fm_feminicidio WHERE NOT es_tentativa) AS consumados,(SELECT COUNT(*) FROM fm_feminicidio WHERE es_tentativa) AS tentativas,(SELECT COUNT(*) FROM sc_punto_microtrafico WHERE estado='activo') AS microtrafico_activo,(SELECT COUNT(*) FROM sc_incidente_convivencia) AS incidentes,(SELECT COUNT(*) FROM pp_denuncia_formal) AS denuncias,(SELECT COUNT(*) FROM fc_falla_institucional) AS fallas,(SELECT COUNT(*) FROM pp_medida_proteccion WHERE cumplida_por_atacante=false) AS medidas_incumplidas""")
    row=cur.fetchone();cur.close();conn.close()
    return {"overview":dict(row)}


# ══════════════════════════════════════════════════════════════════════════════
# ENDPOINTS DE ANÁLISIS GEOGRÁFICO
# ══════════════════════════════════════════════════════════════════════════════

POINT_LAYERS_FOR_ANALYSIS = {
    "ea_lugar_hecho":           {"label":"Lugares del Hecho",     "color":"#FF3860","table":"ea_lugar_hecho",           "geom":"geom","filter":None},
    "ea_lugar_abandono_cuerpo": {"label":"Abandono del Cuerpo",   "color":"#6B21A8","table":"ea_lugar_abandono_cuerpo", "geom":"geom","filter":None},
    "sc_punto_microtrafico":    {"label":"Puntos Microtrafico",   "color":"#FF6B35","table":"sc_punto_microtrafico",    "geom":"geom","filter":"estado = 'activo'"},
    "sc_incidente_convivencia": {"label":"Incidentes Convivencia","color":"#FFCA3A","table":"sc_incidente_convivencia", "geom":"geom","filter":None},
    "em_equipamiento_urbano":   {"label":"Equipamiento Urbano",   "color":"#00C9D4","table":"em_equipamiento_urbano",   "geom":"geom","filter":"activo = true"},
    "tv_lugar_residencia_v":    {"label":"Residencia — Víctima",  "color":"#9B6DFF","table":"tv_lugar_residencia","geom":"geom","filter":"id_victima IS NOT NULL"},
    "tv_lugar_residencia_a":    {"label":"Residencia — Atacante", "color":"#9F1239","table":"tv_lugar_residencia","geom":"geom","filter":"id_atacante IS NOT NULL"},
    "tv_lugar_crianza_v":       {"label":"Crianza — Víctima",     "color":"#22C55E","table":"tv_lugar_crianza",  "geom":"geom","filter":"id_victima IS NOT NULL"},
    "tv_lugar_crianza_a":       {"label":"Crianza — Atacante",    "color":"#DC2626","table":"tv_lugar_crianza",  "geom":"geom","filter":"id_atacante IS NOT NULL"},
    "tv_lugar_trabajo_v":       {"label":"Trabajo — Víctima",     "color":"#FB923C","table":"tv_lugar_trabajo",  "geom":"geom","filter":"id_victima IS NOT NULL"},
    "tv_lugar_trabajo_a":       {"label":"Trabajo — Atacante",    "color":"#991B1B","table":"tv_lugar_trabajo",  "geom":"geom","filter":"id_atacante IS NOT NULL"},
}


@app.get("/analysis/layers")
def get_analysis_layers():
    return {"layers": [
        {"id": k, "label": v["label"], "color": v["color"]}
        for k, v in POINT_LAYERS_FOR_ANALYSIS.items()
    ]}


# ── Helpers: verificar columnas y hacer consultas seguras ─────────────────────
def col_exists(cur, table: str, col: str) -> bool:
    """True si la columna existe en la tabla."""
    cur.execute("""
        SELECT 1 FROM information_schema.columns
        WHERE table_name=%s AND column_name=%s LIMIT 1
    """, (table, col))
    return cur.fetchone() is not None

def table_exists(cur, table: str) -> bool:
    cur.execute("SELECT 1 FROM information_schema.tables WHERE table_name=%s LIMIT 1", (table,))
    return cur.fetchone() is not None

def safe_count(cur, sql: str) -> int:
    """Ejecuta un COUNT y retorna 0 si falla."""
    try:
        cur.execute(sql)
        r = cur.fetchone()
        return int(r[0] if r else 0)
    except Exception:
        cur.connection.rollback()
        return 0

def safe_scalar(cur, sql: str, default=None):
    """Ejecuta una query escalar y retorna default si falla."""
    try:
        cur.execute(sql)
        r = cur.fetchone()
        return r[0] if r and r[0] is not None else default
    except Exception:
        cur.connection.rollback()
        return default


# ── Configuración de filtros por capa ─────────────────────────────────────────
# Cada entrada: {field_name: (tabla_fuente, join_sql_para_obtener_geom)}
# Si la columna está en la misma tabla que geom -> join_sql = None
FILTER_CFG = {
    "ea_lugar_hecho": {
        # modalidad, estado_caso, hora_del_dia, dia_semana viven en fm_feminicidio
        # ea_lugar_hecho.id_feminicidio -> fm_feminicidio.id_feminicidio
        "modalidad":   ("fm_feminicidio", "ea_lugar_hecho lh JOIN fm_feminicidio f ON lh.id_feminicidio = f.id_feminicidio"),
        "estado_caso": ("fm_feminicidio", "ea_lugar_hecho lh JOIN fm_feminicidio f ON lh.id_feminicidio = f.id_feminicidio"),
        "hora_del_dia":("fm_feminicidio", "ea_lugar_hecho lh JOIN fm_feminicidio f ON lh.id_feminicidio = f.id_feminicidio"),
        "dia_semana":  ("fm_feminicidio", "ea_lugar_hecho lh JOIN fm_feminicidio f ON lh.id_feminicidio = f.id_feminicidio"),
    },
    "sc_incidente_convivencia": {
        "tipo_incidente":    ("sc_incidente_convivencia", None),
        "dia_semana":        ("sc_incidente_convivencia", None),
        "subtipo":           ("sc_incidente_convivencia", None),
        "hora_del_dia":      ("sc_incidente_convivencia", None),
    },
    "sc_punto_microtrafico": {
        "tipo_punto":            ("sc_punto_microtrafico", None),
        "tipo_sustancia":        ("sc_punto_microtrafico", None),
        "frecuencia_actividad":  ("sc_punto_microtrafico", None),
        "estado":                ("sc_punto_microtrafico", None),
    },
}


def _get_filter_options_safe(cur, layer_id: str, field: str):
    """Devuelve lista de {value, count} para field en layer_id."""
    cfg_layer = FILTER_CFG.get(layer_id, {})
    field_cfg  = cfg_layer.get(field)
    if not field_cfg:
        return []

    src_table, join_expr = field_cfg
    geom_table = "ea_lugar_hecho" if layer_id == "ea_lugar_hecho" else layer_id
    geom_alias = "lh" if join_expr and "lh" in join_expr else src_table[0:2]

    # Verificar que la columna existe en la tabla origen
    if not col_exists(cur, src_table, field):
        return []

    # Construir query
    if join_expr:
        # JOIN para obtener el campo
        geom_ref = "lh.geom" if "lh" in join_expr else f"{src_table}.geom"
        sql = f"""
            SELECT {field} AS v, COUNT(*) AS n
            FROM {join_expr}
            WHERE {geom_ref} IS NOT NULL AND {field} IS NOT NULL
            GROUP BY {field} ORDER BY n DESC
        """
    else:
        sql = f"""
            SELECT {field} AS v, COUNT(*) AS n
            FROM {src_table}
            WHERE geom IS NOT NULL AND {field} IS NOT NULL
            GROUP BY {field} ORDER BY n DESC
        """
    try:
        cur.execute(sql)
        return [{"value": str(r["v"]), "count": int(r["n"])} for r in cur.fetchall()]
    except Exception as e:
        cur.connection.rollback()
        return []


def _get_filtered_points_safe(cur, layer_id: str, field: str, value: str):
    """Devuelve lista de [lat, lon] filtrada."""
    cfg_layer = FILTER_CFG.get(layer_id, {})
    field_cfg  = cfg_layer.get(field)
    if not field_cfg:
        return []

    src_table, join_expr = field_cfg

    if not col_exists(cur, src_table, field):
        return []

    if join_expr:
        geom_ref = "lh.geom"
        sql = f"""
            SELECT ST_Y(ST_Transform({geom_ref},4326)) AS lat,
                   ST_X(ST_Transform({geom_ref},4326)) AS lon
            FROM {join_expr}
            WHERE {geom_ref} IS NOT NULL AND {field} = %s
        """
    else:
        sql = f"""
            SELECT ST_Y(ST_Transform(geom,4326)) AS lat,
                   ST_X(ST_Transform(geom,4326)) AS lon
            FROM {src_table}
            WHERE geom IS NOT NULL AND {field} = %s
        """
    try:
        cur.execute(sql, (value,))
        return [[float(r["lat"]), float(r["lon"])] for r in cur.fetchall()]
    except Exception as e:
        cur.connection.rollback()
        return []


# ── /analysis/filter_options ──────────────────────────────────────────────────
@app.get("/analysis/filter_options")
def get_filter_options(layer_id: str, field: str):
    if layer_id not in FILTER_CFG:
        raise HTTPException(400, f"Capa {layer_id} no soportada")
    if field not in FILTER_CFG[layer_id]:
        raise HTTPException(400, f"Campo {field} no soportado para {layer_id}")
    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    opts = _get_filter_options_safe(cur, layer_id, field)
    cur.close(); conn.close()
    return {"layer_id": layer_id, "field": field, "options": opts}


# ── /analysis/filter ──────────────────────────────────────────────────────────
@app.get("/analysis/filter")
def analysis_filter(layer_id: str, field: str, value: str):
    if layer_id not in FILTER_CFG:
        raise HTTPException(400, f"Capa {layer_id} no soportada")
    if field not in FILTER_CFG[layer_id]:
        raise HTTPException(400, f"Campo {field} no soportado")
    cfg = POINT_LAYERS_FOR_ANALYSIS.get(layer_id, LAYERS.get(layer_id, {}))
    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    pts  = _get_filtered_points_safe(cur, layer_id, field, value)
    cur.close(); conn.close()
    return {
        "layer_id": layer_id,
        "label":    cfg.get("label", layer_id),
        "color":    cfg.get("color", "#FF3860"),
        "field":    field, "value": value,
        "count":    len(pts), "points": pts,
    }


# ── /analysis/buffer ──────────────────────────────────────────────────────────
@app.get("/analysis/buffer")
def analysis_buffer(layer_id: str, radius_m: float = 200.0):
    if layer_id not in POINT_LAYERS_FOR_ANALYSIS:
        raise HTTPException(404, "Capa no disponible")
    cfg  = POINT_LAYERS_FOR_ANALYSIS[layer_id]
    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cond = f"WHERE {cfg['filter']}" if cfg.get("filter") else ""
    cur.execute(f"""
        SELECT json_build_object(
            'type','FeatureCollection',
            'features',COALESCE(json_agg(
                json_build_object(
                    'type','Feature',
                    'geometry',ST_AsGeoJSON(
                        ST_Transform(ST_Buffer(ST_Transform({cfg['geom']},3857),%s),4326)
                    )::json,
                    'properties','{{}}'::json
                )
            ),'[]'::json)
        ) AS g
        FROM {cfg['table']} {cond}
        {"AND" if cond else "WHERE"} {cfg['geom']} IS NOT NULL
    """, (radius_m,))
    row = cur.fetchone()
    counts = []
    for oid, ocfg in POINT_LAYERS_FOR_ANALYSIS.items():
        if oid == layer_id: continue
        ocond = f"WHERE {ocfg['filter']}" if ocfg.get("filter") else ""
        try:
            cur.execute(f"""
                SELECT COUNT(*) AS n FROM {ocfg['table']} o {ocond}
                {"AND" if ocond else "WHERE"} o.{ocfg['geom']} IS NOT NULL
                AND EXISTS(
                    SELECT 1 FROM {cfg['table']} s {cond}
                    {"AND" if cond else "WHERE"} s.{cfg['geom']} IS NOT NULL
                    AND ST_DWithin(o.{ocfg['geom']}::geography,s.{cfg['geom']}::geography,%s)
                )
            """, (radius_m,))
            n = cur.fetchone()["n"]
            counts.append({"id":oid,"label":ocfg["label"],"color":ocfg["color"],"count":int(n)})
        except Exception:
            cur.connection.rollback()
            counts.append({"id":oid,"label":ocfg["label"],"color":ocfg["color"],"count":0})
    cur.close(); conn.close()
    gj = json.loads(row["g"]) if row and row["g"] else {"type":"FeatureCollection","features":[]}
    return {"layer_id":layer_id,"layer_label":cfg["label"],"layer_color":cfg["color"],
            "radius_m":radius_m,"buffer_geojson":gj,
            "counts":sorted([c for c in counts if c["count"]>0],key=lambda x:-x["count"])}


# ── /analysis/correlate ───────────────────────────────────────────────────────
@app.get("/analysis/correlate")
def analysis_correlate(layer_a: str, layer_b: str, radius_m: float = 300.0):
    for lid in (layer_a, layer_b):
        if lid not in POINT_LAYERS_FOR_ANALYSIS:
            raise HTTPException(404, f"Capa {lid} no disponible")
    cfgA = POINT_LAYERS_FOR_ANALYSIS[layer_a]
    cfgB = POINT_LAYERS_FOR_ANALYSIS[layer_b]
    conn = get_conn(); cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    condA = f"WHERE {cfgA['filter']}" if cfgA.get("filter") else ""
    condB = f"WHERE {cfgB['filter']}" if cfgB.get("filter") else ""
    cur.execute(f"SELECT COUNT(*) AS n FROM {cfgA['table']} {condA} {'AND' if condA else 'WHERE'} {cfgA['geom']} IS NOT NULL")
    total_a = int(cur.fetchone()["n"])
    cur.execute(f"SELECT COUNT(*) AS n FROM {cfgB['table']} {condB} {'AND' if condB else 'WHERE'} {cfgB['geom']} IS NOT NULL")
    total_b = int(cur.fetchone()["n"])
    cur.execute(f"""
        SELECT COUNT(DISTINCT a.ctid) AS n FROM {cfgA['table']} a {condA}
        {"AND" if condA else "WHERE"} a.{cfgA['geom']} IS NOT NULL
        AND EXISTS(SELECT 1 FROM {cfgB['table']} b {condB}
        {"AND" if condB else "WHERE"} b.{cfgB['geom']} IS NOT NULL
        AND ST_DWithin(a.{cfgA['geom']}::geography,b.{cfgB['geom']}::geography,%s))
    """, (radius_m,))
    a_with_b = int(cur.fetchone()["n"])
    try:
        cur.execute(f"""
            SELECT ROUND(AVG(d)::numeric,1) AS avg,ROUND(MIN(d)::numeric,1) AS mn,
                   ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY d)::numeric,1) AS med
            FROM(SELECT ST_Distance(a.{cfgA['geom']}::geography,b.{cfgB['geom']}::geography) AS d
                 FROM {cfgA['table']} a {condA} {"AND" if condA else "WHERE"} a.{cfgA['geom']} IS NOT NULL
                 CROSS JOIN LATERAL(SELECT {cfgB['geom']} FROM {cfgB['table']} {condB}
                     {"AND" if condB else "WHERE"} {cfgB['geom']} IS NOT NULL
                     ORDER BY a.{cfgA['geom']} <-> {cfgB['geom']} LIMIT 1) b) sub
        """)
        dr = cur.fetchone()
    except Exception: cur.connection.rollback(); dr = None
    buckets=[(0,100,"0-100m"),(100,250,"100-250m"),(250,500,"250-500m"),(500,9e9,">500m")]
    dist_chart=[]
    for lo,hi,lbl in buckets:
        try:
            cur.execute(f"""
                SELECT COUNT(DISTINCT a.ctid) AS n FROM {cfgA['table']} a {condA}
                {"AND" if condA else "WHERE"} a.{cfgA['geom']} IS NOT NULL
                CROSS JOIN LATERAL(SELECT ST_Distance(a.{cfgA['geom']}::geography,b.{cfgB['geom']}::geography) AS d
                    FROM {cfgB['table']} b {condB} {"AND" if condB else "WHERE"} b.{cfgB['geom']} IS NOT NULL
                    ORDER BY a.{cfgA['geom']} <-> b.{cfgB['geom']} LIMIT 1) nb WHERE nb.d BETWEEN %s AND %s
            """, (lo,hi))
            dist_chart.append({"label":lbl,"value":int(cur.fetchone()["n"])})
        except Exception: cur.connection.rollback(); dist_chart.append({"label":lbl,"value":0})
    cur.close(); conn.close()
    return {
        "layer_a":{"id":layer_a,"label":cfgA["label"],"color":cfgA["color"],"total":total_a},
        "layer_b":{"id":layer_b,"label":cfgB["label"],"color":cfgB["color"],"total":total_b},
        "radius_m":radius_m,"a_with_b_nearby":a_with_b,
        "pct_a_with_b":round(a_with_b/max(total_a,1)*100,1),
        "avg_dist_m":float(dr["avg"]) if dr and dr["avg"] else None,
        "min_dist_m":float(dr["mn"])  if dr and dr["mn"]  else None,
        "median_dist_m":float(dr["med"]) if dr and dr["med"] else None,
        "dist_chart":dist_chart,
    }


# ── /analysis/sector_profile ──────────────────────────────────────────────────
@app.get("/analysis/sector_profile")
def analysis_sector_profile():
    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    def cnt(sql):  return safe_count(cur, sql)
    def scl(sql):  return safe_scalar(cur, sql, 0)

    fem_total  = cnt("SELECT COUNT(*) FROM fm_feminicidio")
    fem_cons   = cnt("SELECT COUNT(*) FROM fm_feminicidio WHERE es_tentativa = false")
    micro      = cnt("SELECT COUNT(*) FROM sc_punto_microtrafico WHERE estado='activo'")
    incid      = cnt("SELECT COUNT(*) FROM sc_incidente_convivencia")

    # Denuncias archivadas — intentar varios nombres de columna
    den_total  = cnt("SELECT COUNT(*) FROM pp_denuncia_formal")
    den_arch   = 0
    for col_val in [("estado_denuncia","archivada"),("estado","archivada"),("estado","Archivada"),("estado","ARCHIVADA")]:
        c,v = col_val
        if col_exists(cur, "pp_denuncia_formal", c):
            den_arch = cnt(f"SELECT COUNT(*) FROM pp_denuncia_formal WHERE {c}='{v}'")
            if den_arch > 0: break

    # Medidas incumplidas
    med_total  = cnt("SELECT COUNT(*) FROM pp_medida_proteccion")
    med_inc    = 0
    if col_exists(cur,"pp_medida_proteccion","cumplida_por_atacante"):
        med_inc = cnt("SELECT COUNT(*) FROM pp_medida_proteccion WHERE cumplida_por_atacante=false")

    # Fallas sin sancion
    fal_total  = cnt("SELECT COUNT(*) FROM fc_falla_institucional")
    fal_sin    = 0
    if col_exists(cur,"fc_falla_institucional","sancion_aplicada"):
        fal_sin = cnt("SELECT COUNT(*) FROM fc_falla_institucional WHERE sancion_aplicada=false")
    elif col_exists(cur,"fc_falla_institucional","tiene_sancion"):
        fal_sin = cnt("SELECT COUNT(*) FROM fc_falla_institucional WHERE tiene_sancion=false")

    # Tasa feminicidios
    tasa = float(scl("SELECT ROUND(AVG(tasa_por_100k_mujeres)::numeric,1) FROM tot_feminicidios_territoriales") or 0)

    # IPM y Gini — con fallback si la tabla no existe
    ipm_val, gini_val = 0, 0
    if table_exists(cur,"te_indice_socioeconomico"):
        if col_exists(cur,"te_indice_socioeconomico","nombre_indice"):
            ipm_val  = float(scl("SELECT ROUND(AVG(valor)::numeric,1) FROM te_indice_socioeconomico WHERE nombre_indice='IPM'")  or 0)
            gini_val = float(scl("SELECT ROUND(AVG(valor)::numeric,1) FROM te_indice_socioeconomico WHERE nombre_indice='Gini'") or 0)

    cur.close(); conn.close()

    def pct(a, b): return round(int(a) / max(int(b), 1) * 100, 1)
    def cap(v, mx): return min(round(float(v) / max(float(mx), 0.001) * 100, 1), 100)

    indicators = [
        {"label":"Tasa feminicidios",    "value":cap(tasa,50),            "raw":f"{tasa} x 100k mujeres",              "color":"#FF3860"},
        {"label":"Incidencias",          "value":cap(incid,400),          "raw":f"{incid} incidentes registrados",     "color":"#FF6B35"},
        {"label":"Microtrafico",         "value":cap(micro,50),           "raw":f"{micro} puntos activos",             "color":"#FFCA3A"},
        {"label":"Den. archivadas",      "value":pct(den_arch,den_total), "raw":f"{pct(den_arch,den_total)}% del total ({den_arch}/{den_total})", "color":"#8B5CF6"},
        {"label":"Med. incumplidas",     "value":pct(med_inc,med_total),  "raw":f"{pct(med_inc,med_total)}% del total ({med_inc}/{med_total})",  "color":"#6B21A8"},
        {"label":"Fallas sin sancion",   "value":pct(fal_sin,fal_total),  "raw":f"{pct(fal_sin,fal_total)}% del total ({fal_sin}/{fal_total})",  "color":"#EC4899"},
        {"label":"Indice IPM",           "value":cap(ipm_val,100),        "raw":f"{ipm_val}% pobreza multidim.",       "color":"#F59E0B"},
        {"label":"Desigualdad Gini",     "value":cap(gini_val*100,100),   "raw":f"Gini = {gini_val}",                  "color":"#14B8A6"},
    ]
    return {"indicators": indicators, "sector": "El Playon - Rafael Uribe Uribe"}