#!/usr/local/bin/python3.4


## This script is used for debugging scraping scripts

from bs4 import BeautifulSoup, NavigableString, UnicodeDammit
from urllib.request import urlopen
import datetime
import re
import html
from dateutil import parser
import csv

html_doc = """
<html><head><title>The Dormouse's story</title></head>
<body>
<div style="TEXT-ALIGN: justify"><font style="FONT-SIZE: 11pt; FONT-FAMILY: Garamond, serif">11</font><font id="TRGRRTFtoHTMLTab" style="FONT-SIZE: 1px; WIDTH: 36pt; DISPLAY: inline-block">&#160;</font><font style="FONT-SIZE: 11pt; FONT-FAMILY: Garamond, serif">AGGREGATE AMOUNT BENEFICIALLY OWNED BY EACH REPORTING PERSON</font> abc  <div> efg </div></div>
"""


# strip_tags: recursively strips the tag of any node if the tag is included in a tag lists
# The script is adopted from the first answer in http://stackoverflow.com/questions/1765848/remove-a-tag-using-beautifulsoup-but-keep-its-contents
# I use str instead of unicode in the original script because unicode is replaced by string in Python 3
# In addition, if a node has its tags removed, the node's parent will be checked. If the parent contains only NavigableStrings as children, these strings will be
#  concatenated together. The parent node will be modified to contain this joint string as its only content.
# This script is developed to process ICAHN SC13 filings because it uses a lot of <font> tags around target text such as "AGGREGATE BENEFICIALLY OWNED ....."


# The strip_tags script essentially flattens a node and collapse all text in the node (and its subtrees) into one string.
#  It works well if the subtrees are simple and the tags in the subtrees are easily identifiable.
# Another approach is to re-define a find function and search recursively in subtrees. If the target string is within one subtree then this approach might be more efficient.


def strip_tags(thisNode, tags):
    parentNodes = []
    for curTag in thisNode.findAll(True):
        if curTag.name in tags:
            parentNodes += [curTag.parent]
            s = ""
            for c in curTag.contents:
                if not isinstance(c, NavigableString):
                    c = strip_tags(str(c), tags)
                s = s.join(str(c))
            print(len(s))
            curTag.replaceWith(str(s))

    # if all the contents in in this node are navigablestrings, i.e. as a result of striping tags
    # then concatenate all the strings together as the only content in this node
    parentNodes = list(set(parentNodes))
#    print(parentNodes)
    for curParent in parentNodes:
        curContents = curParent.contents
#        print(len(curContents))
        curNavigable = [isinstance(curContent, NavigableString) for curContent in curContents]
        if (all(curNavigable)):
            curStrings = [str(curContent) for curContent in curContents]
            print(curStrings)
            newStr = ' '.join(curStrings)
            curParent.clear()
#            print(curParent.string)
            curParent.string = newStr

    return(thisNode)

##Code used to debug and test the strip_tags function
# soup=BeautifulSoup(html_doc)
# test = soup.find('div')
# print(test.contents)
# for tag in test.contents:
#     print(isinstance(tag, NavigableString))
#     print(tag.name)
# shareCountlink = soup.find('div', text=re.compile('AGGREGATE\s+AMOUNT\s+BENEFICIALLY\s+OWNED', re.I), recursive=True)
# print(shareCountlink)
# print('stripping font tag.......')
#
# soup = strip_tags(soup, ['font'])
# test = soup.find('div')
# print(test.contents)
# print('here')
# print(test.string)
# print('here')
# for tag in test.contents:
#     print(isinstance(tag, NavigableString))
#     print(tag.name)
# shareCountlink = soup.find('div', text=re.compile('AGGREGATE\s+AMOUNT\s+BENEFICIALLY\s+OWNED', re.I))
# print(shareCountlink)
# shareCountlink = soup.find('font', text=re.compile('AGGREGATE\s+AMOUNT\s+BENEFICIALLY\s+OWNED', re.I))
# print(shareCountlink)
##Code used to debug and test the strip_tags function

##recursively search subtrees for a node that contains a specific text
## Inputs: a node, a tag name and a text string
## Output: a list of nodes within the input node that contain the search text and the specified tag name
## 1. If tgtTag and tgtStr are both None, return thisNode
## 2. If thisNode is None, return None
## 3. If tgtStr is None: return all subtrees with the tag, and thisNode if it has a matching tag
## 4. If
def TreeFind (thisNode, tgtTag, tgtStr, debugging=False):
    if thisNode is None:
        return None

    if not (tgtTag or tgtStr):
        return [thisNode]

    #tgtStr is empty, find all subtrees with the particular tag
    outputNodes = []
    if not tgtStr:
        outputNodes = thisNode.findAll(tgtTag)
        if (thisNode.name == tgtTag):
            outputNodes += [thisNode]
        if outputNodes:
            return(list(set(outputNodes)))
        else:
            return(outputNodes)

    #tgtTag is empty, find only subtrees with the particular string
    #or if the tag of the current node matches tgtTag, just check if there is a subtree that contains a navigable string matching the input pattern
    # if such a subtree exists, return thisNode ; in addition, if the subtree also has a tag matching tgtTag, return the subtree as well
    if (debugging):
        print(outputNodes)
    if (not tgtTag) or (thisNode.name == tgtTag):
        for child in thisNode.contents:
            if isinstance(child, NavigableString):
                if debugging:
                    print('comparing strings......')
                if re.search(re.compile(tgtStr, re.I), child.string):
                    if debugging:
                        print(tgtStr)
                        print(child.string)
                    outputNodes += [thisNode]
            else:
                if debugging:
                    print(child)
                    print(tgtStr)
                subtreeNodes = TreeFind(child, '', tgtStr, debugging)
                if subtreeNodes:
                    if tgtTag:
                        for curNode in subtreeNodes:
                            if (curNode.name == tgtTag):
                                outputNodes += [curNode]
                    if not outputNodes:
                        outputNodes += [thisNode]
                if debugging:
                    print('output....')
                    print(outputNodes)
                    print('end of output')
    else:
        #tgtTag and tgtStr are specified, and current Node doesn't match the tgtTag:
        taggedNodes = thisNode.findAll(tgtTag)
        if debugging:
            print(len(taggedNodes))
            print(len(taggedNodes[0].contents))

        print(len(taggedNodes))
        i = 0
        for taggedNode in taggedNodes:
            subtreeNodes = TreeFind(taggedNode, tgtTag, tgtStr, debugging)
            if subtreeNodes:
                i += 1
                outputNodes += subtreeNodes
            print(i)

    return(outputNodes)


def URL2Soup(urlInput):

    soup = None
    try:
        soup = BeautifulSoup(urlopen(urlInput))
    except Exception as e:
        print(e)

    return(soup)


##Code used to debug and test the TreeFind function
print('Debugging tree find')
soup=BeautifulSoup(html_doc)
tmplink = soup.find('div')
print(tmplink)
x = TreeFind(tmplink, 'div', 'test')
print(x)
print('-----')
tmpurl = 'http://www.sec.gov/Archives/edgar/data/859737/000092846416000189/holxsch13damd9041316.htm'
x = TreeFind(URL2Soup(tmpurl), 'div', 'AGGREGATE AMOUNT BENEFICIALLY OWNED')
print(len(x))
# for tmpx in x:
#     tmpy = TreeFind(tmpx,'div', 'AGGREGATE AMOUNT BENEFICIALLY OWNED' )
#     print(len(tmpy))
print('beginning')
print(x[0])
print('end')
TreeFind(x[0], 'div', 'AGGREGATE AMOUNT BENEFICIALLY OWNED', debugging=True)
# y = URL2Soup(tmpurl)
# print(y.name)
#print(x)
##Code used to debug and test the TreeFind function










