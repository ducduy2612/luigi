import luigi
import sys
import tasks

 
if __name__ == '__main__':
    luigi.build([tasks.Complete_Daily_Liquidity_Report(currentdate=sys.argv[1])],workers=2,scheduler_host='localhost')