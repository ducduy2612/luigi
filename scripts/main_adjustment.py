import luigi
import sys
import tasks
 
 
if __name__ == '__main__':
    luigi.build([tasks.Complete_Liquidity_Report_Adjustment(currentdate=sys.argv[1],lagdays=sys.argv[2])],workers=2,scheduler_host='localhost')