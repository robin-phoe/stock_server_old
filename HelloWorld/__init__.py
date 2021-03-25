import pymysql
# pymysql.install_as_MySQLdb()

# 解决版本报错“有一个好办法,直接指定版本,比其他的解决方法简单一些…”
pymysql.version_info = (1, 4, 13, "final", 0)
pymysql.install_as_MySQLdb()  # 使用pymysql代替mysqldb连接数据库