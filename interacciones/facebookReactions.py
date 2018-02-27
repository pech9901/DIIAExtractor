import json
import psycopg2
import interacciones.scrapeFunctions as insf

# Obtiene y almacena todas las reacciones por post
def almacenaFacebookPostsReactions(group_id, access_token, dbConnect):
    num_processed = 0
    cursor = dbConnect.cursor()
    statusIDS = insf.obtieneFacebookPostsIDAuthorID(group_id, access_token, 100 )
    has_next_page_ST = True
    while has_next_page_ST:
        for statusID in statusIDS['data']:
            status_id = statusID['id']
            status_author = statusID['from']['id']
            reactions = insf.obtieneReactionsForPost(status_id, access_token)
            if reactions['data'] != []:
                has_next_page = True
                while has_next_page:
                    for reaction in reactions['data']:
                        # Por ahora dejo el id de Facebook, luego cambio por CI
                        id_author = reaction['id']
                        #Modificar, es solo para pruebas
                        id = status_id+id_author
                        id_nodo_destino = status_author
                        tipo_contenido = '7'
                        text_contenido = reaction['type']
                        reaction_id = status_author+id_author
                        try:
                            cursor.execute("SELECT 1 FROM interaccion WHERE interaccion.id_origen = %s ;",
                                        (reaction_id,))  # Existe el registro?
                            existe_reg = cursor.fetchone()
                            if not existe_reg:
                                cursor.execute(
                                    "INSERT INTO interaccion (id_interaccion, nodo_origen, nodo_destino, tipo_interaccion, id_curso_origen,\
                                    tipo_contenido, contenido, plataforma, id_origen)\
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s); ",
                                    (id, id_author, id_nodo_destino, '3', group_id, tipo_contenido, text_contenido, '1', reaction_id))
                        except psycopg2.Error as e:
                            print("PostgreSQL Error: " + e.diag.message_primary)
                            continue
                        num_processed += 1
                        dbConnect.commit()

                    try:
                        if 'paging' in reactions.keys ( ):
                            reactions = json.loads (insf.request_until_succeed (reactions['paging']['next']))
                    except KeyError:
                        has_next_page = False

        if 'paging' in statusIDS.keys():
            statusIDS = json.loads(insf.request_until_succeed(statusIDS['paging']['next']))
        else:
            has_next_page_ST = False
