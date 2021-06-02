import csv

TargetFunds = [('SagardCapitalPartnersLP', '0001423385'),
               ('Coronation', '0001569143'),
               ('ValuePartners', '0000926614')
##               ('SAC2009', '0001018103'),
##               ('Point72Asia', '0001599822'),
##               ('SAC', '0001451928'),
               ]

TargetCorps = [('LJPC', '0000920465'),
               ('RDUS', '0001428522'),
               ('CSLT', '0001433714'),
               ('TSM', '0001046179'),
               ('JKS', '0001481513'),
               ('VRA', '0001495320'),
               ('ONCS', '0001444307'),
               ('BKD', '0001332349')


              ]




TargetCorps = list(set(TargetCorps))
TargetCorps = sorted(TargetCorps, key=lambda x:x[0])
filename = '/Users/Shared/Models/CorpCIKs.csv'
with open(filename, 'r') as csvfile:
    moreTargetCorps = [tuple(line) for line in csv.reader(csvfile)]
TargetCorps += moreTargetCorps
TargetCorps = list(set(TargetCorps))
TargetCorps = sorted(TargetCorps, key=lambda x:x[0])
with open(filename, 'w', encoding='utf-8') as csvfile:
    writer=csv.writer(csvfile)
    writer.writerows(TargetCorps)

filename = '/Users/Shared/Models/skipCIKs.csv'
with open(filename, 'r', encoding='utf-8') as csvfile:
    SkipCIKs = [tuple(line) for line in csv.reader(csvfile)]
SkipCIKs = sorted(list(set(SkipCIKs)))
with open(filename, 'w', encoding='utf-8') as csvfile:
    writer=csv.writer(csvfile)
    writer.writerows(SkipCIKs)



##Combine the list above and the existing CIKs.csv to create an updated CIKs.csv
##This is the main list of funds that we track 
filename = '/Users/Shared/Models/CIKs.csv'
with open(filename, 'r') as csvfile:
    coreTargetFunds = [tuple(line) for line in csv.reader(csvfile)]
KnownCIKs = set([line[1] for line in TargetFunds])
newCoreFunds = []
for line in coreTargetFunds:
    if line[1] in KnownCIKs:
        continue
    else:
        newCoreFunds +=[line]
        KnownCIKs.update(line[1])

TargetFunds = newCoreFunds + TargetFunds
TargetFunds = list(set(TargetFunds) - set(SkipCIKs))
TargetFunds = sorted(TargetFunds, key=lambda x:x[0])
with open(filename, 'w', encoding='utf-8') as csvfile:
    writer=csv.writer(csvfile)
    writer.writerows(TargetFunds)

##create a fund list from CIKs.csv
UniqueFunds = list(sorted(set([fund[0] for fund in TargetFunds])))
filename = '/Users/Shared/Models/Funds.csv'
with open(filename, 'w', encoding='utf-8') as csvfile:
    writer=csv.writer(csvfile)
    for fund in UniqueFunds :
        writer.writerow([fund])




##Clean up fullCIKs.csv
##1. If any CIK is in CorpCIKs or CIKs ==> remove form fullCIKs
##2. Add CorpCIKs and CIKs to fullCIKs
## This way it ensures that fullCIKs contains all CIKs that has ever been encountered
##  while using assigned names/tickers when possible
KnownCIKs = set(list(set([line[1] for line in TargetFunds])) + list(set([line[1] for line in TargetCorps])))
filename = '/Users/Shared/Models/fullCIKs.csv'
with open(filename, 'r') as csvfile:
    fullTargetFunds = [tuple(line) for line in csv.reader(csvfile)]
newfullFunds = []
for line in fullTargetFunds:
    if line[1] in KnownCIKs:
        continue
    else:
        newfullFunds += [line]
        KnownCIKs.update(line[1])
fullTargetFunds = newfullFunds + TargetFunds + TargetCorps
fullTargetFunds = list(set(fullTargetFunds))
fullTargetFunds = sorted(fullTargetFunds, key=lambda x:x[0])
print(len(fullTargetFunds))
with open(filename, 'w', encoding='utf-8') as csvfile:
    writer=csv.writer(csvfile)
    writer.writerows(fullTargetFunds)


##clean up skipCIKs.csv
## useful when we start tracking individual stock's filings
KnownCIKs = set(list(set([line[1] for line in TargetFunds])) + list(set([line[1] for line in TargetCorps])))
filename = '/Users/Shared/Models/skipCIKs.csv'
with open(filename, 'r') as csvfile:
    fullTargetFunds = [tuple(line) for line in csv.reader(csvfile)]
newfullFunds = []
for line in fullTargetFunds:
    if line[1] in KnownCIKs:
        continue
    else:
        newfullFunds += [line]
        KnownCIKs.update(line[1])
fullTargetFunds = newfullFunds 
fullTargetFunds = list(set(fullTargetFunds))
fullTargetFunds = sorted(fullTargetFunds, key=lambda x:x[0])
print(len(fullTargetFunds))
with open(filename, 'w', encoding='utf-8') as csvfile:
    writer=csv.writer(csvfile)
    writer.writerows(fullTargetFunds)



##Show the number of funds and CIKs 
KnownCIKs = list(set([line[1] for line in TargetFunds]))
print(len(KnownCIKs))
for curCIK in KnownCIKs:
    funds = [tuple(line) for line in TargetFunds if line[1] == curCIK]
    if (len(funds) > 1):
        print(funds)
KnownCIKs = set([line[1] for line in fullTargetFunds])
print(len(KnownCIKs))
for curCIK in KnownCIKs:
    funds = [tuple(line) for line in TargetFunds if line[1] == curCIK]
    if (len(funds) > 1):
        print(funds)



def CheckFullCIKs():
    CIKfile = '/Users/Shared/Models/fullCIKs.csv'

    with open(CIKfile, 'r') as csvfile:
        TargetFunds = [tuple(line) for line in csv.reader(csvfile)]

    CIKs = list(set([line[1] for line in TargetFunds]))
    print(len(TargetFunds))
    print(len(CIKs))

CheckFullCIKs()

