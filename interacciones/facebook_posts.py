import json
import psycopg2
import interacciones.scrape_functions as insf
import interacciones.db_functions as indb


# Dado un post en el grupo en formato JSON devuelve una tupla con los campos del post
def procesaFacebookPosts(status):
    status_id = status['id']
    status_message = '' if 'message' not in status.keys() else \
        insf.unicode_normalize(status['message'])
    link_name = '' if 'name' not in status.keys() else \
        insf.unicode_normalize(status['name'])
    if status['type'] == 'status':
        status_type = 'tex'
    elif status['type'] == 'photo':
        status_type = 'img'
    elif status['type'] == 'link':
        status_type = 'lik'
    elif status['type'] == 'video':
        status_type = 'vid'
    else:
        status_type = 'des'
    status_author = status['from']['id']
    status_published = insf.formatCreatedTime(status['created_time'])
    # Devuelve una tupla con los datos procesados
    return (status_id, status_author, status_message, status_type, status_published)


# Dado el Id de grupo y el access_token, almacena todos los posts del grupo en la base de datos diia
def almacenaFacebookPosts(group_id, access_token, dbConnect):
    cursor = dbConnect.cursor()
    has_next_page = True
    statuses = insf.obtieneFacebookPosts(group_id, access_token, 100)
    while has_next_page:
        for status in statuses['data']:
            # Verifica que status contiene lo esperado
            if 'reactions' in status:
                proceso_status = procesaFacebookPosts(status)
                status_id = proceso_status[0]
                author_facebook_id = proceso_status[1]
                tipo_contenido = proceso_status[3]
                text_contenido = proceso_status[2]
                datatime_published = proceso_status[4]
                # Se trata de posteos, por lo que nodo destino tiene valor Null
                try:
                    cursor.execute("SELECT 1 FROM interaccion WHERE interaccion.id_origen = %s ;", (status_id, ))  # Existe el registro?
                    existe_reg = cursor.fetchone()
                    if not existe_reg:
                        curso_id = indb.getCursoID (group_id,dbConnect)
                        nodo_id = indb.getNodoId(group_id,author_facebook_id,dbConnect)
                        cursor.execute("INSERT INTO interaccion (nodo_origen, tipo_interaccion, id_curso_origen,\
                                tipo_contenido, contenido, plataforma, timestamp, id_origen)\
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s); ", (
                                nodo_id, 'pub', curso_id, tipo_contenido, text_contenido, 'f',
                                datatime_published, status_id))

                except psycopg2.Error as e:
                    print("PostgreSQL Error: " + e.diag.message_primary)
                    continue
            dbConnect.commit()

        # Si no hay más páginas, termina.
        if 'paging' in statuses.keys():
            statuses = json.loads(insf.request_until_succeed(statuses['paging']['next']))
        else:
            has_next_page = False