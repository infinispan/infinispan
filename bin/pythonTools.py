import os
import fnmatch
import re
import subprocess


def toSet(list):
  """Crappy implementation of creating a Set from a List.  To cope with older Python versions"""
  tempDict = {}
  for entry in list:
    tempDict[entry] = "dummy"
  return tempDict.keys()

def getSearchPath(executable):
  """Retrieves a search path based on where teh current executable is located.  Returns a string to be prepended to address any file in the Infinispan src directory."""
  inBinDir = re.compile('^.*/?bin/.*.py')
  if inBinDir.search(executable):
    return "./"
  else:
    return "../"

def stripLeadingDots(filename):
  return filename.strip('/. ')

class GlobDirectoryWalker:
  """A forward iterator that traverses a directory tree"""
  def __init__(self, directory, pattern="*"):
    self.stack = [directory]
    self.pattern = pattern
    self.files = []
    self.index = 0
    
  def __getitem__(self, index):
    while True:
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

class SvnConn(object):
  """An SVN cnnection making use of the command-line SVN client.  Replacement for PySVN which sucked for various reasons."""
  def tag(self, fr_url, to_url, version):
    """Tags a release."""
    checkInMessage = "Infinispan Release Script: Tagging " + version
    subprocess.check_call(["svn", "cp", fr_url, to_url, "-m", checkInMessage])
    
  def checkout(self, url, to_dir):
    """Checks out a URL to the given directory"""
    subprocess.check_call(["svn", "checkout", url, to_dir])
    
  def checkin(self, working_dir, msg):
    """Checks in a working directory with the appropriate message"""
    subprocess.check_call(["svn", "commit", "-m", msg, working_dir])
    
  def add(self, directory):
    """Adds a directory or file to SVN.  Directory can either be the name of a file or dir, or a list of either."""
    if directory:
      call_params = ["svn", "add"]
      if isinstance(directory, str):
        call_params.append(directory)
      else:
        for d in directory:
          call_params.append(d)
      subprocess.check_call(call_params)
      

def get_svn_conn():
  """Factory to create and retrieve an SvnConn instance"""
  return SvnConn()
