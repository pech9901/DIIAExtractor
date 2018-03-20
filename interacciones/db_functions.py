import psycopg2

import rel_idusario_idgrupo as ougi

#Devuelve True si user_id de Facebook es miembro del grupo group_id de Facebook, False en caso contrario
def isgroupmember (user_id,group_id ,dbConnect):
    cursor = dbConnect.cursor ( )
    try:
        cursor.execute (
            "SELECT 1 FROM idusuarioidgrupo WHERE idusuarioidgrupo.id_grupo = %s AND user_facebook_group_id= %s ;",
            (str (group_id),user_id,))
    except psycopg2.Error as e:
        print ("PostgreSQL Error: " + e.diag.message_primary)
    existe_reg = cursor.fetchone ( )
    if existe_reg:
        return True
    else:
        return False

# Obtiene el id de un nodo a partir de su id de grupo de Facebook
def getNodoId (group_id, user_group_id, dbConnect):
    cursor = dbConnect.cursor ( )
    try:
        cursor.execute (
            "SELECT user_facebook_id FROM idusuarioidgrupo WHERE idusuarioidgrupo.id_grupo = %s AND user_facebook_group_id= %s ;",
            (str (group_id),user_group_id,))
        user_id = cursor.fetchone ( )[0]
        cursor.execute ("SELECT 1 FROM estudiante WHERE estudiante.id_facebook= %s ;",(str (user_id),))
        existe_reg= cursor.fetchone()
        if existe_reg:
            cursor.execute ("SELECT id_estudiante FROM estudiante WHERE estudiante.id_facebook= %s ;",(str (user_id),))
            nodo_id = cursor.fetchone ( )[0]
        else:
            cursor.execute ("SELECT id_docente FROM docente WHERE docente.id_facebook= %s ;",(str (user_id),))
            nodo_id = cursor.fetchone ( )[0]
    except psycopg2.Error as e:
        print ("PostgreSQL Error: " + e.diag.message_primary)
    dbConnect.commit ( )
    return nodo_id

# Obtiene el id de un curso a partir de su id de grupo en Facebook
def getCursoID (group_id, dbConnect):
    cursor = dbConnect.cursor ( )
    try:
        cursor.execute ("SELECT id_curso FROM curso WHERE curso.id_facebook= %s ;",
                                        (str (group_id),))
        curso_id = cursor.fetchone ( )[0]
    except psycopg2.Error as e:
        print ("PostgreSQL Error: " + e.diag.message_primary)
    dbConnect.commit ( )
    return curso_id


