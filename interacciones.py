import psycopg2
import interacciones.facebook_posts as infp
import interacciones.facebook_comments as infc
import interacciones.facebook_reactions as infr



# Id de grupo - debe obtenerse de la tabla cursos
#group_id = Valid group_id1
#access_token = Valid access_token



# Conexión a la Base de datos
try:
    dbConnect = psycopg2.connect("dbname='diia' user='postgres' host='localhost' password='123'")
except Exception as e:
    print(e)


def getFacebookInteractions(group_id, access_token, dbConnect):
    infp.almacenaFacebookPosts(group_id, access_token, dbConnect)
    infc.almacenaFacebookPostsComments(group_id, access_token, dbConnect)
    infr.almacenaFacebookPostsReactions (group_id,access_token,dbConnect)
    dbConnect.close()


if __name__ == '__main__':
    getFacebookInteractions(group_id, access_token, dbConnect)
