import psycopg2
import interacciones.facebookPosts as infp
import interacciones.facebookComments as infc


# Id de grupo - debe obtenerse de la tabla cursos
group_id = 1268404236573400
access_token = "1205790082775855|701e1f6d986822a19cf4f579868a2d72"

# Conexión a la Base de datos
try:
    dbConnect = psycopg2.connect("dbname='diia' user='postgres' host='localhost' password='123'")
except Exception as e:
    print(e)


def getFacebookInteractions(group_id, access_token, dbConnect):
    infp.almacenaFacebookPosts(group_id, access_token, dbConnect)
    infc.almacenaFacebookPostsComments(group_id, access_token, dbConnect)
    dbConnect.close()


if __name__ == '__main__':
    getFacebookInteractions(group_id, access_token, dbConnect)