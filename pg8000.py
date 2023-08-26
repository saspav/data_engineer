class Cursor:
    def __init__(self, *args, **kwargs):
        pass

    def execute(self, *args, **kwargs):
        pass


class Dummy:
    def __init__(self, *args, **kwargs):
        pass

    def cursor(self, *args, **kwargs):
        return Cursor()

    def commit(self, *args, **kwargs):
        pass

    def close(self, *args, **kwargs):
        pass


class pg8000:
    def __init__(self, *args, **kwargs):
        pass

    @staticmethod
    def connect(*args, **kwargs):
        return Dummy()


def connect(*args, **kwargs):
    return Dummy()


if __name__ == "__main__":
    client = 'none_client'
    opened = '01.01.2000 00:00'
    table_name = f"lab10_aaa_2"

    dbname = 'databases'
    user = 'user_name'
    pswd = 'user_pass'
    host = 'host_urls'
    port = 'host_port'
    conn = pg8000.connect(database=dbname, user=user, password=pswd, host=host, port=port)
    cursor = conn.cursor()
    query = "INSERT INTO {} (client, opened) VALUES (%s, %s)".format(table_name)
    print(query)
    cursor.execute(query, (client, opened))
    conn.commit()
    conn.close()
