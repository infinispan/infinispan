import os
import fnmatch
import re

### Crappy implementation of creating a Set from a List.  To cope with older Python versions
def toSet(list):
    tempDict = {}
    for entry in list:
        tempDict[entry] = "dummy"
    return tempDict.keys()

def getSearchPath(executable):
    inBinDir = re.compile('^.*/?bin/.*.py')
    if inBinDir.search(executable):
        return "./"
    else:
        return "../"

def stripLeadingDots(filename):
    return filename.strip('/. ')

class GlobDirectoryWalker:
    # a forward iterator that traverses a directory tree

    def __init__(self, directory, pattern="*"):
        self.stack = [directory]
        self.pattern = pattern
        self.files = []
        self.index = 0

    def __getitem__(self, index):
        while 1:
            try:
                file = self.files[self.index]
                self.index = self.index + 1
            except IndexError:
                # pop next directory from stack
                self.directory = self.stack.pop()
                self.files = os.listdir(self.directory)
                self.index = 0
            else:
                # got a filename
                fullname = os.path.join(self.directory, file)
                if os.path.isdir(fullname) and not os.path.islink(fullname):
                    self.stack.append(fullname)
                if fnmatch.fnmatch(file, self.pattern):
                    return fullname
