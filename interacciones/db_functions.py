import psycopg2

import get_group_users_IDs as ougi

# Id de grupo - debe obtenerse de la tabla cursos
group_id = 1268404236573400
access_token = "1205790082775855|701e1f6d986822a19cf4f579868a2d72"

# Conexión a la Base de datos
try:
    dbConnect = psycopg2.connect("dbname='diia' user='postgres' host='localhost' password='123'")
except Exception as e:
    print(e)


# Obtiene el id de un nodo a partir de su id de grupo de Facebook
def getNodoId (group_id, user_group_id, dbConnect):
    cursor = dbConnect.cursor ( )
    try:
        cursor.execute (
            "SELECT user_facebook_id FROM estudiantegrupofacebook WHERE estudiantegrupofacebook.id_grupo = %s AND user_facebook_group_id= %s ;",
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


if __name__ == '__main__':
    grupo_face_ids = ougi.getGroupMembers (group_id,access_token)
    miembros_face_ids= ougi.processGroupMembers (grupo_face_ids)
    curso= getCursoID(group_id,dbConnect)

    for info in miembros_face_ids:
        ci=getNodoId (group_id,info['id'],dbConnect)
        print(ci)
