import psycopg2
import configparser
from pathlib import Path
from cjio import cityjson
from cjio.models import CityObject, Geometry

config = configparser.ConfigParser()
config.read('config.ini')

host = config['DEFAULT']['host']
schema = config['DEFAULT']['schema']
table = config['DEFAULT']['table']
dbname = config['DEFAULT']['dbname']
user = config['DEFAULT']['user']

cm = cityjson.CityJSON()

def simpletocjio(geom):
    surface = geom.replace('LINESTRING Z (','',1).replace(')','').split(',')
    s = []
    for point in surface:
        c = point.split(' ')
        c = map(lambda v: float(v),c)
        s.append(tuple(c))
    return s

def runquery(query, type):
    global cm
    conn = psycopg2.connect(
        host=host,
        database=dbname,
        user=user
    )
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    conn.commit()
    cur.close()
    conn.close()
    
    for row in rows:
        bdry = [[]]
        
        for g in row[4]:
            bdry[0].append([simpletocjio(g)])

        co = CityObject(
            id=row[2]
        )

        co_attrs = {
            'blockid': row[1],
            'other_attribute': 'bla bla'
        }
        co.attributes = co_attrs
        geom = Geometry(type='Solid', lod=1)
        geom.boundaries = bdry

        co.geometry.append(geom)
        
        #-------------------
        srf = {
            0:{'surface_idx': [], 'type': 'WallSurface'},
            1:{'surface_idx': [], 'type': 'GroundSurface'},
            2:{'surface_idx': [], 'type': 'RoofSurface'}
        }
        
        for i,color in enumerate(row[3]):
            if color == '#F8F8f8':
                srf[0]['surface_idx'].append([0,i])
            elif color == '#FF9823':
                srf[2]['surface_idx'].append([0,i])
            else:
                srf[2]['surface_idx'].append([0,i])
        geom.surfaces = srf
        
        #-------------------------
        
        co.type = type
        cm.cityobjects[co.id] = co

    cityobjects, vertex_lookup = cm.reference_geometry()
    cm.add_to_j(cityobjects,vertex_lookup)
    return 0




query = """
WITH sub AS (
    SELECT gridid,
        blockid,
        id,
        colors,
        (ST_Dump(geom)).*
    FROM {}.{} 
    WHERE ST_Intersects(geom_2d,ST_MakeEnvelope(117366.5,560858.7, 119841.7,562233.9,28992))
    --LIMIT 100 --safety
)
SELECT 
    gridid,
    blockid,
    id,
    colors,
    array_agg(ST_AsText(ST_ExteriorRing(geom))) geom
    FROM sub
    GROUP BY gridid,
    blockid,
    id,
    colors   
;
""".format(schema,table)

runquery(query, 'Building')

#--- end of buildings

query = """
SELECT gridid, 
    id as blockid, 
    id, 
    ARRAY['#F8F8f8'] as colors,
    array_agg(ST_AsText(ST_ExteriorRing(ST_Reverse(geom)))) geom
FROM results.bgtvlakkenz
WHERE ST_Intersects(geom,ST_MakeEnvelope(117366.5,560858.7, 119841.7,562233.9,28992))
AND landuse != 'groenvoorziening'
GROUP BY gridid, blockid, id, colors;
"""
runquery(query,'LandUse')

#--- Plantcover
query = """
SELECT gridid, 
    id as blockid, 
    id, 
    ARRAY['#F8F8f8'] as colors,
    array_agg(ST_AsText(ST_ExteriorRing(ST_Reverse(geom)))) geom
FROM results.bgtvlakkenz
WHERE ST_Intersects(geom,ST_MakeEnvelope(117366.5,560858.7, 119841.7,562233.9,28992))
AND landuse = 'groenvoorziening'
GROUP BY gridid, blockid, id, colors;
"""
runquery(query,'PlantCover')



def addmaterial():
    #ADd materials to cm.j.cityObjects.materials
    arr = cm.j['CityObjects']
    for o in arr:
        #print(cm.j['CityObjects'][o])
        pass
        #cm.j['CityObjects'][o]["material"] = {
        #    "roof": { 
        #        "values": [[0, 0, 1, null]] 
                #"""
                #TODO cm.j['CityObjects'][o]['semantics']['surfaces'] -> find values from:
                #'semantics': {
                #    'surfaces': [
                #        {'type': 'WallSurface'}, 
                #        {'type': 'GroundSurface'}, 
                #        {'type': 'RoofSurface'}
                #    ], 
                #    'values': [
                #        [2, 2, 2, 2, 2, 2, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
                #    ]
                #}
                #"""
        #    },
        #    "wall": { 
        #        "values": [[2, 2, 1, null]] 
        #    }
        #}


cm.j["materials"] = [
  {
    "name": "roof",
    "ambientIntensity":  0.2000,
    "diffuseColor":  [0.9000, 0.1000, 0.7500],
    "emissiveColor": [0.9000, 0.1000, 0.7500],
    "specularColor": [0.9000, 0.1000, 0.7500],
    "shininess": 0.2,
    "transparency": 0.5,
    "isSmooth": False
  },
  {
    "name": "wall",
    "ambientIntensity":  0.4000,
    "diffuseColor":  [0.1000, 0.1000, 0.9000],
    "emissiveColor": [0.1000, 0.1000, 0.9000],
    "specularColor": [0.9000, 0.1000, 0.7500],
    "shininess": 0.0,
    "transparency": 0.5,
    "isSmooth": True
  }            
]

cm.update_bbox()
cm.validate()

outfile = 'mycitymodel.json'
cityjson.save(cm, outfile)
