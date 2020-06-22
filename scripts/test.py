import luigi
import sys
import utils
import os


class Task1(luigi.Task):
    
    def requires(self):
        return None
    
    def output(self):
        return luigi.LocalTarget('task1')
 
    def run(self):
        with self.output().open('w') as outfile:
            outfile.write('DONE ALL')
 
class Task2(luigi.Task):
    
    def requires(self):
        return Task1()
    
    def output(self):
        return luigi.LocalTarget('task2')
 
    def run(self):
        with self.output().open('w') as outfile:
            outfile.write('DONE ALL')
 
if __name__ == '__main__':
    luigi.build([Task2()],workers=2,scheduler_host='localhost')