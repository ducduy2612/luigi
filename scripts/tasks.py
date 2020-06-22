import time
import pyodbc
import luigi
import utils
import os


connString_114 = r'Driver={SQL Server Native Client 11.0};Server=10.36.96.114;Database=Phoenix_Conf;Trusted_Connection=yes;'
connString_116 = r'Driver={SQL Server Native Client 11.0};Server=10.36.96.116;Database=Core;Trusted_Connection=yes;'


class Daily_Liqui_Data_Ready_Check(luigi.Task):
    businessdate = luigi.Parameter()
    upstream = luigi.Parameter()
    filespath = luigi.Parameter()
    
    def requires(self):
        return self.upstream
    
    def output(self):
        return luigi.LocalTarget(self.filespath + '/Daily_Liqui_Data_Ready_Check_' + self.businessdate)
    
    def run(self):
        status = ''
        cnxnStatus = pyodbc.connect(connString_116)
        cursorStatus = cnxnStatus.cursor()
        while status != 'Ready':
            cursorStatus.execute("SELECT IIF(count(1) > 0, 'Ready', 'Wait') FROM core.ref.table_control WHERE Business_Date = "+ self.businessdate +" AND table_name = 'cash_flow_dqc'")
            status = str(cursorStatus.fetchone()[0])
            if status != 'Ready':
                time.sleep(60)
        cnxnStatus.close()
        with self.output().open('w') as outfile:
            outfile.write('Data ready check status: ' + status)


class Liqui_Adjustment_Upload_Check(luigi.Task):
    businessdate = luigi.Parameter()
    upstream = luigi.Parameter()
    filespath = luigi.Parameter()
    
    def requires(self):
        return self.upstream
    
    def output(self):
        return luigi.LocalTarget(self.filespath + '/Liqui_Adjustment_Upload_Check_' + self.businessdate)
    
    def run(self):
        status = ''
        cnxnStatus = pyodbc.connect(connString_114)
        cursorStatus = cnxnStatus.cursor()
        while status != 'Ready':
            cursorStatus.execute("""SELECT IIF(COUNT(*)>0, 'Ready', 'Wait') FROM 
                                    Phoenix_Conf.app.Request_Object iv
                                    INNER JOIN
                                    Phoenix_Conf.app.Approve_Status iv1
                                    	ON iv.PK_ID = iv1.Fk_Request_ID
                                    WHERE
                                    	iv.Comment = 'LIQUI_RPT_""" + self.businessdate.replace("'","").replace("-","") +"""'
                                        AND iv1.Status = 'Approved'""")
            status = str(cursorStatus.fetchone()[0])
            if status != 'Ready':
                time.sleep(60)
        cnxnStatus.close()
        with self.output().open('w') as outfile:
            outfile.write('Data ready check status: ' + status)


class Loader_Batch_10191(luigi.Task):
    businessdate = luigi.Parameter()
    upstream = luigi.Parameter()
    filespath = luigi.Parameter()
    
    def requires(self):
        return self.upstream
    
    def output(self):
        return luigi.LocalTarget(self.filespath + '/Loader_Batch_10191_' + self.businessdate)
    
    def run(self):
        cnxn = pyodbc.connect(connString_114)
        cursor = cnxn.cursor()
        cnxn.autocommit = True
        query = "SET NOCOUNT ON; EXEC phoenix_conf.tech.run_Batch_Loader 10191," + self.businessdate + " , NULL"
        cursor.execute(query)
        status = utils.check_batch_loader_status('10191', self.businessdate)
        with self.output().open('w') as outfile:
            outfile.write('Loader status: ' + status)


class Extractor_Batch_10185(luigi.Task):
    businessdate = luigi.Parameter()
    upstream = luigi.Parameter()
    filespath = luigi.Parameter()
    
    def requires(self):
        return self.upstream
    
    def output(self):
        return luigi.LocalTarget(self.filespath + '/Extractor_Batch_10185_' + self.businessdate)
    
    def run(self):
        cnxn = pyodbc.connect(connString_114)
        cursor = cnxn.cursor()
        cnxn.autocommit = True
        query = "SET NOCOUNT ON; EXEC phoenix_conf.tech.run_Batch_Extractor 10185," + self.businessdate + " , NULL"
        cursor.execute(query)
        status = utils.check_batch_extractor_status('10185',self.businessdate)
        with self.output().open('w') as outfile:
            outfile.write('Extractor status: ' + status)


class Loader_Batch_167(luigi.Task):
    businessdate = luigi.Parameter()
    upstream = luigi.Parameter()
    filespath = luigi.Parameter()
    
    def requires(self):
        return self.upstream
    
    def output(self):
        return luigi.LocalTarget(self.filespath + '/Loader_Batch_167_' + self.businessdate)
    
    def run(self):
        cnxn = pyodbc.connect(connString_114)
        cursor = cnxn.cursor()
        cnxn.autocommit = True
        query = "SET NOCOUNT ON; EXEC phoenix_conf.tech.run_Batch_Loader 167," + self.businessdate + " , NULL"
        cursor.execute(query)
        status = utils.check_batch_loader_status('167', self.businessdate)
        with self.output().open('w') as outfile:
            outfile.write('Loader status: ' + status)
            

class Engine_Batch_5__COTR(luigi.Task):
    businessdate = luigi.Parameter()
    upstream = luigi.Parameter()
    filespath = luigi.Parameter()
    
    def requires(self):
        return self.upstream
 
    def output(self):
        return luigi.LocalTarget(self.filespath + '/Engine_Batch_5__COTR_' + self.businessdate)
 
    def run(self):
        status = utils.run_batch_engine(self.businessdate, '5', "'VPBANK'", "'SBV'", "'Regular'", "0")
        with self.output().open('w') as outfile:
            outfile.write('Engine status: ' + status)


class Engine_Batch_10174__LCCF_LRCF_ICCF(luigi.Task):
    businessdate = luigi.Parameter()
    upstream = luigi.Parameter()
    filespath = luigi.Parameter()
    
    def requires(self):
        return self.upstream
 
    def output(self):
        return luigi.LocalTarget(self.filespath + '/Engine_Batch_10174__LCCF_LRCF_ICCF_' + self.businessdate)
 
    def run(self):
        status = utils.run_batch_engine(self.businessdate, '10174', "'VPBANK'", "'SBV'", "'Regular'", "0")
        with self.output().open('w') as outfile:
            outfile.write('Engine status: ' + status)


class Engine_Batch_10177__LMRCF_LiquiRPT_LRCF3(luigi.Task):
    businessdate = luigi.Parameter()
    upstream = luigi.Parameter()
    filespath = luigi.Parameter()
    
    def requires(self):
        return self.upstream
 
    def output(self):
        return luigi.LocalTarget(self.filespath + '/Engine_Batch_10177__LMRCF_LiquiRPT_LRCF3_' + self.businessdate)
 
    def run(self):
        status = utils.run_batch_engine(self.businessdate, '10177', "'VPBANK'", "'LRCF_3'", "'Regular'", "0")
        with self.output().open('w') as outfile:
            outfile.write('Engine status: ' + status)


class Engine_Batch_10178__LiquiRPTAdj_LRCF3(luigi.Task):
    businessdate = luigi.Parameter()
    upstream = luigi.Parameter()
    filespath = luigi.Parameter()
    
    def requires(self):
        return self.upstream
 
    def output(self):
        return luigi.LocalTarget(self.filespath + '/Engine_Batch_10178__LiquiRPTAdj_LRCF3_' + self.businessdate)
 
    def run(self):
        status = utils.run_batch_engine(self.businessdate, '10178', "'VPBANK'", "'LRCF_3'", "'Liqui_Adj'", "0")
        with self.output().open('w') as outfile:
            outfile.write('Engine status: ' + status)
            
            
class Engine_Batch_10175__LMRCF_LiquiRPT_LRCF2(luigi.Task):
    businessdate = luigi.Parameter()
    upstream = luigi.Parameter()
    filespath = luigi.Parameter()
    
    def requires(self):
        return self.upstream
 
    def output(self):
        return luigi.LocalTarget(self.filespath + '/Engine_Batch_10175__LMRCF_LiquiRPT_LRCF2_' + self.businessdate)
 
    def run(self):
        status = utils.run_batch_engine(self.businessdate, '10175', "'VPBANK'", "'LRCF_2'", "'Regular'", "0")
        with self.output().open('w') as outfile:
            outfile.write('Engine status: ' + status)
            

class Engine_Batch_172__LiquiRPTAdj_LRCF2(luigi.Task):
    businessdate = luigi.Parameter()
    upstream = luigi.Parameter()
    filespath = luigi.Parameter()
    
    def requires(self):
        return self.upstream
 
    def output(self):
        return luigi.LocalTarget(self.filespath + '/Engine_Batch_172__LiquiRPTAdj_LRCF2_' + self.businessdate)
 
    def run(self):
        status = utils.run_batch_engine(self.businessdate, '172', "'VPBANK'", "'LRCF_2'", "'Liqui_Adj'", "0")
        with self.output().open('w') as outfile:
            outfile.write('Engine status: ' + status)


class Engine_Batch_171__LiquiRPT_LRCF1(luigi.Task):
    businessdate = luigi.Parameter()
    upstream = luigi.Parameter()
    filespath = luigi.Parameter()
    
    def requires(self):
        return self.upstream
 
    def output(self):
        return luigi.LocalTarget(self.filespath + '/Engine_Batch_171__LiquiRPT_LRCF1_' + self.businessdate)
 
    def run(self):
        status = utils.run_batch_engine(self.businessdate, '171', "'VPBANK'", "'LRCF_1'", "'Regular'", "0")
        with self.output().open('w') as outfile:
            outfile.write('Engine status: ' + status)


class Complete_Daily_Liquidity_Report(luigi.Task):
    currentdate = luigi.Parameter()
    businessdate = luigi.Parameter(default=None)
    filespath = luigi.Parameter(default=None)
    
    def __init__(self,*arg,**kwargs):
        super(Complete_Daily_Liquidity_Report,self).__init__(*arg,**kwargs)
        self.businessdate=utils.get_last_working_day(self.currentdate)
        self.filespath = os.path.join(os.getcwd(), r'data/' + self.businessdate.replace("'",""))
        if not os.path.exists(self.filespath):
           os.makedirs(self.filespath)
    
    def requires(self):
        taskList = {0:'Daily_Liqui_Data_Ready_Check',
                    10185:'Extractor_Batch_10185',
                    167:'Loader_Batch_167',
                    5:'Engine_Batch_5__COTR',
                    10174:'Engine_Batch_10174__LCCF_LRCF_ICCF',
                    10177:'Engine_Batch_10177__LMRCF_LiquiRPT_LRCF3',
                    10175:'Engine_Batch_10175__LMRCF_LiquiRPT_LRCF2',
                    171:'Engine_Batch_171__LiquiRPT_LRCF1'}
                    
        result = utils.build_tree(taskList=taskList,businessdate=self.businessdate,filespath=self.filespath,useconfig=True,additionalDependency={10185:0})
        return [result['Engine_Batch_171__LiquiRPT_LRCF1'], result['Engine_Batch_10175__LMRCF_LiquiRPT_LRCF2'], result['Engine_Batch_10177__LMRCF_LiquiRPT_LRCF3']]
 
    def output(self):
        return luigi.LocalTarget(self.filespath + '/Complete_Daily_Liquidity_Report_' + self.businessdate)
 
    def run(self):
        cnxn = pyodbc.connect(connString_114)
        cursor = cnxn.cursor()
        query = """declare
                        @v_msg VARCHAR(255),
                        @v_sender VARCHAR(255) = 'VPBANK\\tuanna52',
                        @v_msg_type VARCHAR(255) = 'INFO',
                        @v_receiver VARCHAR(255) = 'VPBANK\\tuanna52,VPBANK\duynd13,VPBANK\leptm2,VPBANK\linhpt34,VPBANK\chaulh1,VPBANK\haianh'
                    set @v_msg = CONCAT('Liquidity Reports are available for business_date = """ + self.businessdate.replace("'","")  +""" at ', getdate())
                   
                    exec job.[app].[Send_Notifi_By_Users]
                        @v_sender,
                        @v_msg_type,
                        @v_receiver,
                        @v_msg"""
        cursor.execute(query)
        with self.output().open('w') as outfile:
            outfile.write('DONE ALL')


class Complete_Liquidity_Report_Adjustment(luigi.Task):
    currentdate = luigi.Parameter()
    lagdays = luigi.Parameter()
    businessdate = luigi.Parameter(default=None)
    filespath = luigi.Parameter(default=None)
    
    def __init__(self,*arg,**kwargs):
        super(Complete_Liquidity_Report_Adjustment,self).__init__(*arg,**kwargs)
        self.businessdate=utils.get_last_working_day(self.currentdate, self.lagdays)
        self.filespath = os.path.join(os.getcwd().replace("\\scripts","\\data/"), self.businessdate.replace("'",""))
        if not os.path.exists(self.filespath):
           os.makedirs(self.filespath)
    
    def requires(self):
        taskList = {0:'Liqui_Adjustment_Upload_Check',
                    10191:'Loader_Batch_10191',
                    10178:'Engine_Batch_10178__LiquiRPTAdj_LRCF3',
                    172:'Engine_Batch_172__LiquiRPTAdj_LRCF2'}
        result = utils.build_tree(taskList=taskList,businessdate=self.businessdate,filespath=self.filespath,useconfig=False,additionalDependency={10191:0, 10178:10191, 172:10191})
        return [result['Engine_Batch_10178__LiquiRPTAdj_LRCF3'], result['Engine_Batch_172__LiquiRPTAdj_LRCF2']]
 
    def output(self):
        return luigi.LocalTarget(self.filespath + '/Complete_Liquidity_Report_Adjustment_' + self.businessdate)
 
    def run(self):
        cnxn = pyodbc.connect(connString_114)
        cursor = cnxn.cursor()
        query = """declare
                        @v_msg VARCHAR(255),
                        @v_sender VARCHAR(255) = 'VPBANK\\tuanna52',
                        @v_msg_type VARCHAR(255) = 'INFO',
                        @v_receiver VARCHAR(255) = 'VPBANK\\tuanna52,VPBANK\duynd13,VPBANK\leptm2,VPBANK\linhpt34,VPBANK\chaulh1,VPBANK\haianh'
                    set @v_msg = CONCAT('Liquidity Reports with adjustments are available for business_date = """ + self.businessdate.replace("'","") +""" at ', getdate())
                   
                    exec job.[app].[Send_Notifi_By_Users]
                        @v_sender,
                        @v_msg_type,
                        @v_receiver,
                        @v_msg"""
        print(query)
        cursor.execute(query)
        with self.output().open('w') as outfile:
            outfile.write('DONE ALL')