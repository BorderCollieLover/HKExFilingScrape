from glob import glob
import os
import csv
import re
import datetime


##targetdir = '/Users/shared/fund clone/*'
targetdir = '/Users/shared/OptionData/*'

for subdir in glob(targetdir):
    print(subdir)
    if os.path.isdir(subdir):
        try:
            os.rmdir(subdir)
        except Exception as e:
            print(e)




def FileMissingorStale(filename):
    if not os.path.exists(filename):
        return True

    filetime = datetime.datetime.fromtimestamp( os.path.getmtime(filename))
    if ((datetime.datetime.now() - filetime) > datetime.timedelta(hours=3)):
        return True
    else:
        return False

CIKfile = '/Users/Shared/Models/fullCIKs.csv'
with open(CIKfile, 'r') as csvfile:
    TargetFunds = [tuple(line) for line in csv.reader(csvfile)]

def CleanUppered13Files():
    for fund in TargetFunds:
        # print(fund)
        HldgDir = '/Users/shared/fund clone/' + fund[0]
        if not os.path.exists(HldgDir):
            continue

        p = re.compile('[,\s]')
        HldgFiles = glob(HldgDir + '/*.csv')

        for HldgFile in HldgFiles:
            if not FileMissingorStale(HldgFile):
                print(HldgFile)
                os.remove(HldgFile)

#CleanUppered13Files()
