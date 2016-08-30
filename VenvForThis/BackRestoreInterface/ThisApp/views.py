from django.shortcuts import render, render_to_response, redirect
from django.http import HttpResponse
import pyodbc
import time
from datetime import datetime
from ThisApp.models import *
from os import listdir, path
import os


def mssql_connect(name):
    connection = pyodbc.connect(driver='{SQL Server}',
                                server='oirc-srv-test04', Trusted_Connection=True, autocommit=True)
    connection1 = pyodbc.connect(driver='{SQL Server}',
                                server='oirc-srv-test04', Trusted_Connection=True, autocommit=True)
    storage_path = r"\\oirc-srv-nas2\k$\30.03.16_all_DB\SVO_db\\" + name
    sql = "RESTORE DATABASE [{0}] FROM DISK = '{1}' WITH REPLACE".format(os.path.splitext(path.basename(name))[0],
                                                                         storage_path)
    cur = connection.cursor()
    cur1 = connection1.cursor()
    thistime = time.strftime("%d_%m_%Y_%H_%M_%S_", time.gmtime())

    size_query = """
    RESTORE HEADERONLY FROM DISK = '{0}'
    """.format(storage_path)

    restore_query = """
    DECLARE @Query AS VARCHAR(1000) = '
        RESTORE DATABASE [{0}]
        FROM  DISK = N''{1}''
        WITH  FILE = 1,
        MOVE N''ASPConvert'' TO N''E:\data\\{2}EVD_SVO_ETL.mdf'',
        MOVE N''ASPNazn'' TO N''E:\data\\{2}EVD_SVO_ETL_NAZN.ndf'',
        MOVE N''ASPNach'' TO N''E:\data\\{2}EVD_SVO_ETL_NACH.ndf'',
        MOVE N''ASPVpl'' TO N''E:\data\\{2}EVD_SVO_ETL_VPL.ndf'',
        MOVE N''ASPScanDoc'' TO N''E:\data\\{2}EVD_SVO_ETL_SCANDOC.ndf'',
        MOVE N''ASPProtocol'' TO N''E:\data\\{2}EVD_SVO_ETL_PROTOCOL.ndf'',
        MOVE N''ASPSocCard'' TO N''E:\data\\{2}EVD_SVO_ETL_SOCCARD.ndf'',
        MOVE N''ASPConvert_log'' TO N''E:\data\\{2}EVD_SVO_ETL_log.ldf'',
        NOUNLOAD,  REPLACE, RECOVERY'
    EXEC(@Query)
    """.format(os.path.splitext(path.basename(name))[0],
               storage_path,
               thistime)

    progress_query = """
    SELECT r.session_id,r.command,CONVERT(NUMERIC(6,2),r.percent_complete)
    AS [PercentComplete],CONVERT(VARCHAR(20),
    DATEADD(ms,r.estimated_completion_time,GetDate()),20) AS [ETA Completion Time],
    CONVERT(NUMERIC(10,2),r.total_elapsed_time/1000.0/60.0) AS [Elapsed Min],
    CONVERT(NUMERIC(10,2),r.estimated_completion_time/1000.0/60.0) AS [ETA Min],
    CONVERT(NUMERIC(10,2),r.estimated_completion_time/1000.0/60.0/60.0) AS [ETA Hours],
    CONVERT(VARCHAR(1000),(SELECT SUBSTRING(text,r.statement_start_offset/2,
    CASE WHEN r.statement_end_offset = -1 THEN 1000 ELSE (r.statement_end_offset-r.statement_start_offset)/2 END)
    FROM sys.dm_exec_sql_text(sql_handle)))
    FROM sys.dm_exec_requests r WHERE command IN ('RESTORE DATABASE','BACKUP DATABASE')
    """

    state_query = """
    SELECT name, user_access_desc, is_read_only, state_desc, recovery_model_desc
    FROM sys.databases WHERE name = '{0}'
    """.format(os.path.splitext(path.basename(name))[0])

    insert_listdb_query = """
    delete from [master].[dbo].[ListDb] where name = '{0}'
    insert [master].[dbo].[ListDb] values(null, '{0}', '{0}', null, null,
    null, null, null, null, null, null, null, -1, null, 0)
    """.format(os.path.splitext(path.basename(name))[0])
    cur.execute(size_query)
    for row_header in cur.fetchall():
            print("""
    BackupType: {0}
    ServerName: {1}
    DatabaseName: {2}
    DatabaseCreationDate: {3}
    BackupSize: {4}
    BackupStartDate: {5}
    CompressedBackupSize: {6}
    SummarySize: {7}
            """.format(row_header[2], row_header[8], row_header[9], row_header[11], row_header[12], row_header[17],
                       row_header[51], str((int(row_header[12]) + int(row_header[51])))))
    cur.execute(restore_query)
    cur1.execute(state_query)
    cur1.execute(progress_query)
    while cur.nextset():
        cur1.execute(progress_query)
        for row_progress in cur1.fetchall():
            print('ID: {0} | Decimal: {1}'.format(row_progress[0], row_progress[2]))
    cur.execute(state_query)
    row = cur.fetchone()
    state = ''
    if row is not None:
        state = row[3]
    if 'ONLINE' in state:
        cur.execute(insert_listdb_query)
    cur1.close()
    cur.close()
    connection.close()


def get_backup_dir():
    files = listdir(r"\\oirc-srv-nas2\k$\30.03.16_all_DB\SVO_db\\")
    after_filter = filter(lambda x: x.endswith('.bak'), files)
    return after_filter


def get_list_db():
    connection = pyodbc.connect(driver='{SQL Server}',
                                server='oirc-srv-test04', Trusted_Connection=True, autocommit=True)
    sql = "SELECT id, alias FROM [master].[dbo].[ListDb]"
    cur = connection.cursor()
    cur.execute(sql)
    list_db = []
    for row in cur.fetchall():
        list_db.append(row.alias)
    cur.close()
    connection.close()
    return list_db


def index(request):
    # a = Publication.objects.get()
    return HttpResponse(
        render_to_response('index.html', {'backup_dir': list(get_backup_dir()), 'list_db': list(get_list_db())}))


def restore(request):
    try:
        for back in range(len(request.GET.getlist('backup'))):
            mssql_connect(request.GET.getlist('backup')[back])
        return HttpResponse(render_to_response('success.html', {}))
    except Exception as ThisEx:
        return HttpResponse(render_to_response('error.html', {'ThisEx': ThisEx}))


def remove_from_listdb(name):
    connection = pyodbc.connect(driver='{SQL Server}',
                                server='oirc-srv-test04', Trusted_Connection=True, autocommit=True)
    remove_listdb_query = "delete from [master].[dbo].[ListDb] where name = '{0}'".\
        format(name)
    cur = connection.cursor()
    cur.execute(remove_listdb_query)
    cur.close()
    connection.close()


def drop_database(name):
    connection = pyodbc.connect(driver='{SQL Server}',
                                server='oirc-srv-test04', Trusted_Connection=True, autocommit=True)
    drop_query = """
    use master
    alter database [{0}] set single_user with rollback immediate
    drop database [{0}]
    """.format(name)
    cur = connection.cursor()
    cur.execute(drop_query)
    cur.close()
    connection.close()


def remove_database(request):
    try:
        for i in range(len(request.GET.getlist('base'))):
            try:
                drop_database(request.GET.getlist('base')[i])
                remove_from_listdb(request.GET.getlist('base')[i])
            except Exception as ThisEx:
                return HttpResponse(render_to_response('error.html', {'ThisEx': ThisEx}))
        return HttpResponse(
            render_to_response('index.html', {'backup_dir': list(get_backup_dir()), 'list_db': list(get_list_db())}))
    except Exception as ThisEx:
        return HttpResponse(render_to_response('error.html', {'ThisEx': ThisEx}))
