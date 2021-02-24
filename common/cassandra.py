from configparser import ConfigParser
from getpass import getuser
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.query import dict_factory


def get_conf(conn_name='cassandra'):
    config = ConfigParser()
    config.read("/mnt/home/{user}/.odbc.ini".format(user=getuser()))
    return config[conn_name]


def create_cassandra_session():
    conf = get_conf()
    auth_provider = PlainTextAuthProvider(username=conf['uid'], password=conf['pwd'])
    cluster = Cluster(conf['hostname'].split(','), port=int(conf['port']), auth_provider=auth_provider)
    session = cluster.connect(keyspace=conf['keyspace'])
    session.row_factory = dict_factory
    return session


def get_job_params(job_name=None, conf_name=None):
    if job_name is None:
        return None
    else:
        if conf_name is None:
            session = create_cassandra_session()
        else:
            session = create_cassandra_session(conf_name)
        result = session.execute("SELECT param_map FROM job_conf_master where job_name='{job_name}'".format(job_name=job_name))
        for row in result:
            params = dict(row['param_map'])
        return params