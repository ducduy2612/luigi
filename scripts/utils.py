import time
import pyodbc

connString = r'Driver={SQL Server Native Client 11.0};Server=10.36.96.114;Database=Phoenix_Conf;Trusted_Connection=yes;'


# get last working day based on input date
def get_last_working_day(currentdate, lagdays = '-1'):
    cnxn = pyodbc.connect(connString)
    cursor = cnxn.cursor()
    cnxn.autocommit = True
    query = "select tech.func_get_workingday(" + currentdate + ", "+ lagdays + ")"
    cursor.execute(query)
    businessdate = str(cursor.fetchone()[0])
    cnxn.close()
    return "'" + businessdate + "'"


# import task based on taskname from module tasks.py
def task_import(taskname):
    tasks = __import__('tasks')
    task = getattr(tasks,taskname)
    return task


# build dependency tree of all tasks
def build_tree(taskList,businessdate,filespath,useconfig=True,additionalDependency={}):
    tasks = {}
    result = {}
    cnxn = pyodbc.connect(connString)
    cursor = cnxn.cursor()
    cnxn.autocommit = True

    for taskid, taskname in taskList.items():
        # build upstreamTaskNameList for each taskname based on config table 
        upstreamTaskNameList=[]
        if useconfig:
            query = "select * from Phoenix_Conf.app.engine_batch_prereq where FK_Batch_ID = " + str(taskid)
            cursor.execute(query)
            upstreamList = [item[2] for item in cursor.fetchall()]
            for upstreamTaskid, upstreamTaskname in taskList.items():
                if upstreamTaskid in upstreamList:
                    upstreamTaskNameList.append(upstreamTaskname)
        # add in additional dependency
        if taskid in additionalDependency.keys():
            upstreamTaskNameList.append(taskList[additionalDependency[taskid]])
        # package as a pair in tasks dictionary
        tasks.update({taskname: upstreamTaskNameList})
    
    # create tree based on tasks dict
    while len(result) != len(taskList):
        for task in tasks:
            upstreamTaskList = tasks[task]
            try:
                result[task] = task_import(task)(businessdate=businessdate,filespath=filespath,upstream=[result[upstreamTask] for upstreamTask in upstreamTaskList])
            except KeyError:
                pass
    return result
            

def check_batch_engine_status(batchId, businessdate):
    status = ''
    cnxnStatus = pyodbc.connect(connString)
    cursorStatus = cnxnStatus.cursor()
    n=0
    while status != 'Completed':
        if n==0:
            time.sleep(5)
            cursorStatus.execute("select top 1 pk_id from Phoenix_Conf.app.Engine_Batch_Log where FK_Batch_ID = " + batchId + " and Business_Date = "+ businessdate +" order by pk_id desc")
            pk_id = cursorStatus.fetchone()[0]
            n+=1
        else:
            cursorStatus.execute("select batch_status, number_failed from Phoenix_Conf.app.Engine_Batch_Log where pk_id = " + str(pk_id))
            resultSet = cursorStatus.fetchone()
            [status, numberfailed] = [resultSet[0], resultSet[1]]
            if status not in ['Completed','Executing','Pending'] or numberfailed > 0:
                raise RuntimeError('Task is ' + status)
            time.sleep(20)

    cnxnStatus.close()
    return status


def check_batch_extractor_status(batchId, businessdate):
    status = ''
    cnxnStatus = pyodbc.connect(connString)
    cursorStatus = cnxnStatus.cursor()
    n=0
    while status != 'Completed':
        if n==0:
            time.sleep(5)
            cursorStatus.execute("select top 1 pk_id from Phoenix_Conf.app.Extractor_Batch_Log where FK_Batch_ID = " + batchId + " and Business_Date = "+ businessdate +" order by pk_id desc")
            pk_id = cursorStatus.fetchone()[0]
            n+=1
        else:
            cursorStatus.execute("select batch_status from Phoenix_Conf.app.Extractor_Batch_Log where pk_id = " + str(pk_id))
            status = str(cursorStatus.fetchone()[0])
            if status not in ['Completed','Executing','Pending']:
                raise RuntimeError('Task is ' + status)
            time.sleep(20)
    cnxnStatus.close()
    return status


def check_batch_loader_status(batchId, businessdate):
    status = ''
    cnxnStatus = pyodbc.connect(connString)
    cursorStatus = cnxnStatus.cursor()
    n=0
    while status != 'Validated':
        if n==0:
            time.sleep(5)
            cursorStatus.execute("select top 1 pk_id from Phoenix_Conf.app.loader_Batch_Log where FK_Batch_ID = " + batchId + " and Business_Date = "+ businessdate +" order by pk_id desc")
            pk_id = cursorStatus.fetchone()[0]
            n+=1
        else:
            cursorStatus.execute("select batch_status from Phoenix_Conf.app.loader_Batch_Log where pk_id = " + str(pk_id))
            status = str(cursorStatus.fetchone()[0])
            if status not in ['Completed','Executing','Validated','Pending']:
                raise RuntimeError('Task is ' + status)
            time.sleep(20)
    cnxnStatus.close()
    return status


def run_batch_engine(businessdate, batch_id, entity_code, approach, scenario_id, version_id):
    cnxn = pyodbc.connect(connString)
    cursor = cnxn.cursor()
    cnxn.autocommit = True
    query = ",".join(["SET NOCOUNT ON; EXEC Phoenix_Conf.tech.run_engine_job " + batch_id, businessdate, entity_code, approach, scenario_id, version_id])
    cursor.execute(query)
    status = check_batch_engine_status(batch_id, businessdate)
    return status