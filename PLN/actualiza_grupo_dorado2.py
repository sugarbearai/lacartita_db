import sys
import pymysql
from bs4 import BeautifulSoup

def inserta(_id, titulo, contenido, categorias, tags):
    #Crea conexion hacia la nueva base de datos grupo_dorado
    try:
        conn = pymysql.connect(host='lacartita.com', user='chicano', passwd='lacartita',
                                                                db='grupo_dorado')
        cur = conn.cursor()
        tit = pymysql.escape_string(titulo)
        conte = pymysql.escape_string(contenido)
        #Inserta en tabla posts
        query = "INSERT INTO posts VALUES(default,"+str(_id)+",'"+tit+"', \
                                                            '"+conte+"');"
        cur.execute(query)
        id_post = cur.lastrowid
        conn.commit()

        #Consigo ID de categorias
        #Verifica si la categoria existe de lo contrario la Inserta
        id_cats =  []        
        for cat in categorias:
            try:
                query = "SELECT category_id FROM categories WHERE category_name\
                                                                ='"+ cat +"';"
                cur.execute(query)
                aux = cur.fetchone()
                id_cats.append(aux[0])
            except:
                sql = "INSERT INTO categories VALUES(default,'"+cat+"');"
                cur.execute(sql)
                id_cats.append(cur.lastrowid)
                conn.commit()
        
        #Consigo ID de tags
        #Verifica si el tag existe de lo contrario la Inserta
        id_tags =  []        
        for tag in tags:
            try:
                query = "SELECT tag_id FROM tags WHERE tag_name\
                                                                ='"+ tag +"';"
                cur.execute(query)
                aux = cur.fetchone()
                id_tags.append(aux[0])
            except:
                sql = "INSERT INTO tags VALUES(default,'"+tag+"');"
                cur.execute(sql)
                id_tags.append(cur.lastrowid)
                conn.commit()

        #Actualiza tablas puente
        for i in id_cats:
            try:
                sql = "INSERT INTO categories_posts VALUES("+str(i)+","+str(id_post)+");"
                cur.execute(sql)
                conn.commit()
            except Exception as e:
                print("Error al actualizar puente categories_posts: ", e)
                pass

        for i in id_tags:
            try:
                sql = "INSERT INTO tags_posts VALUES("+str(i)+","+str(id_post)+");"
                cur.execute(sql)
                conn.commit()
            except Exception as e:
                print("Error al actualizar puente tags_posts: ", e)
                pass
        #Cierro la conexion
        conn.close()

    except Exception as e:
        print("Error durante operaciones en grupo_dorado: ", e)

#Crea conexion a cartita Y consigo el ultimo ID de post insertado
try:
    conexion = pymysql.connect(host='lacartita.com', user='chicano', passwd='lacartita',
                                                                db='grupo_dorado')
    cursor = conexion.cursor()
    try:
        sql = "SELECT ID from posts ORDER BY post_id DESC limit 1"
        cursor.execute(sql)
        last_id = cursor.fetchone()
        last_id = str(last_id[0])
        conexion.close()
    except:
        last_id = "20"

except Exception as e:
    print("Error durante la obtencion de ID en BD grupo_dorado: ", e)
    sys.exit(1)

#Crea conexion hacia bugg_wp7
try:
    conexion = pymysql.connect(host='lacartita.com', user='chicano', passwd='lacartita',
                                                                    db='bugg_wp7')
    cursor = conexion.cursor()
except Exception as e:
    print("No se pudo conectar a bugg_wp7: ", e)
    sys.exit(1)
    
#Consigue las ultimas noticias insertadas
try:
    sql = "SELECT ID, post_title, post_date FROM wp_posts WHERE post_status = 'publish' \
        AND ID > " + last_id + " ORDER BY ID ASC"
    cursor.execute(sql)
    ids = cursor.fetchall()
    if len(ids) == 0:
        print("No hay noticias que actualizar")
        exit(1)
except Exception as e:
    print("Error durante la obtencion de las ultimas noticias: ", e)
    sys.exit(1)

#Para cada noticia conseguimos, ID, titulo, contenido
#categorias y Etiquetas
for _id, t in ids:
    query = "SELECT P.post_title, P.post_content, T.description, C.name \
        FROM wp_posts P INNER JOIN wp_term_relationships R ON P.ID=R.object_id\
        INNER JOIN wp_term_taxonomy T ON R.term_taxonomy_id=T.term_taxonomy_id\
        INNER JOIN wp_terms C ON T.term_id = C.term_id \
        WHERE P.post_status = 'publish' AND P.ID = " + str(_id)

    try:
        cursor.execute(query)
        result = cursor.fetchall()
        #Si la notica con el ID actual no tiene datos (hay ID vacios) salta al sig.
        if len(result) == 0:
            continue       
        titulo = result[0][0]
        #Si el titulo es vacio salta al sig.
        if titulo == '':
            continue
        #Quito etiquetas html
        contenido = BeautifulSoup(result[0][1], "lxml").text
        categorias = set()
        tags = set()

        #Guardo las categorias y etiquetas en Sets (Evita duplicidad)
        for t, c, cat, tag in result:
            if cat != '':
                categorias.add(cat)
            tags.add(tag)

    except Exception as e:
        print("No se pudo extraer datos de: ", _id, e)
        sys.exit(1)

    #Llama a la funcion para insertar la noticia a la nueva base de datos
    inserta(_id, titulo, contenido, categorias, tags)
#Cierro la conexion
conexion.close()
