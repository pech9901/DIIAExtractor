import json

import psycopg2

import interacciones.scrape_functions as insf

# Id de grupo - debe obtenerse de la tabla cursos
#group_id = Valid group_id1268404236573400
#access_token = Valid access_token

# Conexión a la Base de datos
try:
    dbConnect = psycopg2.connect("dbname='diia' user='postgres' host='localhost' password='123'")
except Exception as e:
    print(e)



#Obtiene todos los miembros del grupo dado, devuelve json
def getGroupMembers(group_id, access_token):
    base = "https://graph.facebook.com"
    node = "/%s/members" % group_id
    fields = "?fields=name"
    parameters = "&access_token=%s" % (access_token)
    url = base + node +fields+ parameters
    data = json.loads( insf.request_until_succeed( url ) )
    return data

#Recibe json con los miembors del grupo y devuelve una lista con el nombre y id de grupo de cada uno
def processGroupMembers (members):
    miembros = []
    has_next_page = True
    while has_next_page:
        for member in members['data']:
            miembros.append(member)
        if 'paging' in members.keys():
            if 'next' in members['paging']:
                members = json.loads( insf.request_until_succeed( members['paging']['next'] ) )
            else:
                has_next_page = False
        else:
            has_next_page = False
    return miembros


#Dado el usuario de Facebook devuelve el id y el nombre del usuario en el momento de la consulta
def getUserInfo (id, access_token) :
    base = 'https://graph.facebook.com'
    node= "/%s" % id
    fields = "?fields=id,name"
    parameters = "&access_token=%s" % (access_token)
    url = base + node + fields + parameters
    info = json.loads( insf.request_until_succeed( url ) )
    return info

# Dado un id de grupo en facebook devuelve una lista con todos los id de Facebook (únicos) de los miembros del grupo
def getGroupMembersFaceIds(group_id, dbConnect):
    cursor = dbConnect.cursor ( )
    try:
        cursor.execute ("SELECT id_curso FROM curso WHERE curso.id_facebook = %s ;",(str(group_id), ))
        id_curso = cursor.fetchone ( )[0]
        cursor.execute("SELECT estudiante.id_facebook FROM estudiante, alumnocurso WHERE estudiante.id_estudiante = alumnocurso.id_estudiante AND alumnocurso.id_curso= "+ str(id_curso))
        ids=cursor.fetchall()
        #Obtengo los docentes
        cursor.execute (
            "SELECT docente.id_facebook FROM docente, cursodocente WHERE docente.id_docente = cursodocente.id_docente AND cursodocente.id_curso= " + str (
                id_curso))
        #agrego a los docentes
        ids.append(cursor.fetchall())
    except psycopg2.Error as e:
        print ("PostgreSQL Error: " + e.diag.message_primary)
        pass
    return ids

#Dado el id de un grupo y el id de facebook de un usuario, obtienen el id de ese usuario en el grupo dado y almacena el registro en la tabla estudiantegrupofacebook
def almacenaUserGroupId (group_id, user_id, access_token, dbConnect):
    cursor = dbConnect.cursor()
    miembros = getGroupMembers(group_id,access_token)
    miembros_list=processGroupMembers(miembros)
    for member_data in miembros_list:
        user_data=getUserInfo(user_id, access_token)
        if user_data['name'] == member_data['name']:
            try:
                cursor.execute("SELECT 1 FROM idusuarioidgrupo WHERE idusuarioidgrupo.user_facebook_group_id = %s ;",(member_data['id'],))#Existe el registro?
                existe_reg = cursor.fetchone()
                if not existe_reg:
                    cursor.execute(
                                    "INSERT INTO idusuarioidgrupo (id_grupo, user_facebook_id, user_facebook_group_id) VALUES (%s, %s, %s); ",
                                    (group_id, user_data['id'], member_data['id']))
            except psycopg2.Error as e:
                print("PostgreSQL Error: " + e.diag.message_primary)
                continue
            dbConnect.commit()

if __name__ == '__main__':
    miembros_face_ids = getGroupMembersFaceIds (group_id,dbConnect)
    for info in miembros_face_ids:
        almacenaUserGroupId(group_id, info[0], access_token, dbConnect)
