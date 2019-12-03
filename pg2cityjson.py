import psycopg2
import json
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
bbox = [111664.8,559448.0, 113415.9,560217.5]

cm = cityjson.CityJSON()

def simpletocjio(geom):
    surface = geom.replace('LINESTRING Z (','',1).replace(')','').split(',')
    s = []
    for point in surface:
        c = point.split(' ')
        c = map(lambda v: float(v),c)
        s.append(tuple(c))
    return s

def runquery(query):
    
    global cm
    conn = psycopg2.connect(
        host=host,
        database=dbname,
        user=user
    )
    cur = conn.cursor()
    cur.execute("CREATE SEQUENCE IF NOT EXISTS counter;")
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
        mat = {
            "irradiation": { 
                "values": [[]] 
            }
        }
        for i,color in enumerate(row[3]):
            if color == '#F8F8f8':
                mat['irradiation']['values'][0].append(1)
            elif color == '#FF9823':
                mat['irradiation']['values'][0].append(0)
            else:
                mat['irradiation']['values'][0].append(0)
        geom.material = mat


        #-------------------------
        co.type = row[5]
        cm.cityobjects[co.id] = co

    cityobjects, vertex_lookup = cm.reference_geometry()
    cm.add_to_j(cityobjects,vertex_lookup)
    return 0




buildingquery = """
WITH sub AS (
    SELECT gridid,
        blockid,
        id,
        colors,
        (ST_Dump(geom)).*
    FROM results.extruded 
    WHERE ST_Intersects(geom_2d,ST_MakeEnvelope({},{},{},{},28992))
    --LIMIT 100 --safety
)
SELECT 
    gridid,
    blockid,
    nextval('counter') as id,
    colors,
    array_agg(ST_AsText(ST_ExteriorRing(geom))) geom,
    'Building' AS type
    FROM sub
    GROUP BY gridid,
    blockid,
    id,
    colors   
;
""".format(bbox[0],bbox[1],bbox[2],bbox[3])
runquery(buildingquery)

landquery = """
    WITH sub AS (
        SELECT gridid,
            id as blockid,
            id,
            ARRAY['#000000'] as colors,
            type,
            geom
        FROM results.bgtvlakkenz 
        WHERE ST_Intersects(geom,ST_MakeEnvelope({},{},{},{},28992))
        --LIMIT 100 --safety
    )
    SELECT 
        gridid,
        blockid,
        nextval('counter') as id,
        colors,
        array_agg(ST_AsText(ST_Reverse(ST_ExteriorRing(geom)))) geom,
        CASE 
            WHEN type IN ('onbegroeidterreindeel') THEN 'LandUse'
            WHEN type IN ('begroeidterreindeel') THEN 'PlantCover'
            WHEN type IN ('wegdeel','ondersteunendwegdeel') THEN 'Road'
            WHEN type IN ('waterdeel','ondersteunendwaterdeel') THEN 'WaterBody'
        END as type    
        FROM sub
        GROUP BY gridid,
        blockid,
        id,
        type,
        colors   
    ;
    """.format(bbox[0],bbox[1],bbox[2],bbox[3])
runquery(landquery)

#ADd materials to cm.j.cityObjects.materials
arr = cm.j['CityObjects']
matarray = []
for o in arr:
    
    #cm.j['CityObjects'][o]['geometry'][0]["material"] = 
    matarray.append({
        "roof": {
            "values": cm.j['CityObjects'][o]['geometry'][0]['semantics']['values']
        }
    })
    #print(cm.j['CityObjects'][o])

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
    "name": "blue",
    "ambientIntensity":  0.2000,
    "diffuseColor":  [0.9000, 0.1000, 0.7500],
    "emissiveColor": [0.9000, 0.1000, 0.7500],
    "specularColor": [0.9000, 0.1000, 0.7500],
    "shininess": 0.2,
    "transparency": 0.5,
    "isSmooth": False
  },
  {
    "name": "red",
    "ambientIntensity":  0.4000,
    "diffuseColor":  [0.1000, 0.1000, 0.9000],
    "emissiveColor": [0.1000, 0.1000, 0.9000],
    "specularColor": [0.9000, 0.1000, 0.7500],
    "shininess": 0.0,
    "transparency": 0.5,
    "isSmooth": True
  },
  {
    "name": "green",
    "ambientIntensity":  0.2000,
    "diffuseColor":  [0.9000, 0.1000, 0.7500],
    "emissiveColor": [0.9000, 0.1000, 0.7500],
    "specularColor": [0.9000, 0.1000, 0.7500],
    "shininess": 0.2,
    "transparency": 0.5,
    "isSmooth": False
  }
]
print('Update bbox')
cm.update_bbox()
print('Validating')

outfile = 'mycitymodel.json'
#cityjson.save(cm, outfile)

cityobjects, vertex_lookup = cm.reference_geometry()
cm.add_to_j(cityobjects, vertex_lookup)
#Add materials
for mat in matarray:
    cm.j['CityObjects'][o]['geometry'][0]["material"] = mat

cm.remove_duplicate_vertices()
cm.remove_orphan_vertices()
#cm.validate()

try:
    with open(outfile, 'w') as fout:
        json_str = json.dumps(cm.j, indent=2)
        fout.write(json_str)
except IOError as e:
    raise IOError('Invalid output file: %s \n%s' % (path, e))
