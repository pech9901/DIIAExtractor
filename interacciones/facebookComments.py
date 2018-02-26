import json
import psycopg2
import interacciones.scrapeFunctions as insf


# Dado el id de un post y su arista /comments en formato JSON devuelve una tupla con los datos de la arista
def procesaFacebookComment(comment, status_id):
    comment_id = status_id + comment['id']
    comment_message = '' if 'message' not in comment else comment['message']
    comment_author = comment['from']['id']
    if 'attachment' in comment:
        attach_tag = "%s" % comment['attachment']['type']
        comment_message = attach_tag if comment_message is '' else \
            (comment_message + " " + attach_tag).encode("utf-8")
        if attach_tag == '':
            comment_type = '1'
        elif attach_tag == 'photo':
            comment_type = '2'
        elif attach_tag == 'share':
            comment_type = '3'
        elif attach_tag == 'video_inline' or attach_tag == 'video_share_youtube':
            comment_type = '4'
        elif attach_tag == 'animated_image_share':
            comment_type = '5'
        elif attach_tag == 'sticker':
            comment_type = '6'
        else:
            comment_type = '10'
    else:
        comment_type = '1'
    comment_published = insf.formatCreatedTime(comment['created_time'])

    return (comment_author, comment_type, comment_message, comment_published, comment_id)


def almacenaSubComments(comment, author_id, access_token, group_id, dbConnect):
    cursor = dbConnect.cursor()
    if 'comments' in comment:
        for x in range(1, comment['comment_count']):
            has_next_subpage = True
            parent_id = comment['id']
            subcomments = insf.obtieneFacebookObjectComments(parent_id, access_token, 100)
            while has_next_subpage:
                for subcomment in subcomments['data']:
                    proceso_subcomment = (procesaFacebookComment(subcomment, parent_id))
                    id = subcomment['id'] + str(x)
                    # Por ahora dejo el id de Facebook, luego cambio por CI
                    author_facebook_id = proceso_subcomment[0]
                    id_nodo_destino = author_id
                    tipo_contenido = proceso_subcomment[1]
                    text_contenido = proceso_subcomment[2]
                    datatime_published = proceso_subcomment[3]
                    comment_id = proceso_subcomment[4]
                    try:
                        cursor.execute("SELECT 1 FROM interaccion WHERE interaccion.id_origen = %s ;",
                                        (comment_id,))  # Existe el registro?
                        existe_reg = cursor.fetchone()
                        if not existe_reg:
                            cursor.execute(
                                "INSERT INTO interaccion (id_interaccion, nodo_origen, nodo_destino, tipo_interaccion, id_curso_origen,\
                            tipo_contenido, contenido, plataforma, timestamp, id_origen)\
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s); ",
                                (id, author_facebook_id, id_nodo_destino, '4', group_id, tipo_contenido, text_contenido, '1',
                                 datatime_published, comment_id))
                    except psycopg2.Error as e:
                        print("PostgreSQL Error: " + e.diag.message_primary)
                        continue
                    dbConnect.commit()
                if 'paging' in subcomments:
                    if 'next' in subcomments['paging']:
                        subcomments = json.loads(insf.request_until_succeed(subcomments['paging']['next']))
                    else:
                        has_next_subpage = False
                else:
                    has_next_subpage = False


# Recupera todos los comentarios del grupo por post
def almacenaFacebookPostsComments(group_id, access_token, dbConnect):
    num_processed = 0
    cursor = dbConnect.cursor()
    statusIDS = insf.obtieneFacebookPostsIDAuthorID(group_id, access_token, 100)
    has_next_page_ST = True
    id = 1000
    while has_next_page_ST:
        for statusID in statusIDS['data']:
            has_next_page = True
            status_id = statusID['id']
            status_author = statusID['from']['id']
            comments = insf.obtieneFacebookObjectComments(status_id, access_token, 100)
            while has_next_page and comments is not None:
                for comment in comments['data']:
                    id = id + 1
                    proceso_comment = procesaFacebookComment(comment, status_id)
                    # Por ahora dejo el id de Facebook, luego cambio por CI
                    author_facebook_id = proceso_comment[0]
                    id_nodo_destino = status_author
                    tipo_contenido = proceso_comment[1]
                    text_contenido = proceso_comment[2]
                    datatime_published = proceso_comment[3]
                    comment_id = proceso_comment[4]
                    try:
                        cursor.execute("SELECT 1 FROM interaccion WHERE interaccion.id_origen = %s ;",
                                        (comment_id,))  # Existe el registro?
                        existe_reg = cursor.fetchone()
                        if not existe_reg:
                            cursor.execute(
                                "INSERT INTO interaccion (id_interaccion, nodo_origen, nodo_destino, tipo_interaccion, id_curso_origen,\
                                tipo_contenido, contenido, plataforma, timestamp, id_origen)\
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s); ",
                                (id, author_facebook_id, id_nodo_destino, '4', group_id, tipo_contenido, text_contenido, '1',
                                 datatime_published, comment_id))
                    except psycopg2.Error as e:
                        print("PostgreSQL Error: " + e.diag.message_primary)
                        continue
                    num_processed += 1
                    dbConnect.commit()
                    almacenaSubComments(comment, author_facebook_id, access_token, group_id, dbConnect)
                if 'paging' in comments:
                    if 'next' in comments['paging']:
                        comments = json.loads(insf.request_until_succeed(
                            comments['paging']['next']))
                    else:
                        has_next_page = False
                else:
                    has_next_page = False

        if 'paging' in statusIDS.keys():
            statusIDS = json.loads(insf.request_until_succeed(statusIDS['paging']['next']))
        else:
            has_next_page_ST = False
