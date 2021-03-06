import json
import psycopg2
import interacciones.scrape_functions as insf
import interacciones.db_functions as indb

# Obtiene y almacena todas las reacciones por post
def almacenaFacebookPostsReactions(group_id, access_token, dbConnect):

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
                        id_author = reaction['id']
                        #Determina si el autor de la reacción es miembro del grupo. El grupo es abierto y permite reacciones de no miembros
                        if indb.isgroupmember(id_author, group_id, dbConnect):
                            tipo_contenido = 'rea'
                            text_contenido = reaction['type']
                            reaction_id = status_author+id_author
                            try:
                                cursor.execute("SELECT 1 FROM interaccion WHERE interaccion.id_origen = %s ;",
                                        (reaction_id,))  # Existe el registro?
                                existe_reg = cursor.fetchone()
                                if not existe_reg:
                                    curso_id = indb.getCursoID (group_id,dbConnect)
                                    nodo_id = indb.getNodoId (group_id,id_author,dbConnect)
                                    id_nodo_destino = indb.getNodoId (group_id,status_author,dbConnect)
                                    cursor.execute(
                                     "INSERT INTO interaccion (nodo_origen, nodo_destino, tipo_interaccion, id_curso_origen,\
                                     tipo_contenido, contenido, plataforma, id_origen)\
                                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s); ",
                                     (nodo_id, id_nodo_destino, 'rea', curso_id, tipo_contenido, text_contenido, 'f', reaction_id))
                            except psycopg2.Error as e:
                                print("PostgreSQL Error: " + e.diag.message_primary)
                                continue

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
