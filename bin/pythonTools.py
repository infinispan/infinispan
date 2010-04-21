import os
import fnmatch
import re
import subprocess
import sys
import readline

settings_file = '%s/.infinispan_dev_settings' % os.getenv('HOME')

### Known config keys
svn_base_key = "svn_base"
local_tags_dir_key = "local_tags_dir"
local_mvn_repo_dir_key = "local_mvn_repo_dir"
maven_pom_xml_namespace = "http://maven.apache.org/POM/4.0.0"

def get_settings():
  """Retrieves user-specific settings for all Infinispan tools.  Returns a dict of key/value pairs, or an empty dict if the settings file doesn't exist."""
  f = None
  try:
    settings = {}
    f = open(settings_file)
    for l in f:
      if not l.strip().startswith("#"):
        kvp = l.split("=")
        if kvp and len(kvp) > 0 and kvp[0] and len(kvp) > 1:
          settings[kvp[0].strip()] = kvp[1].strip()
    return settings
  except IOError as ioe:
    return {}
  finally:
    if f:
      f.close()

settings = get_settings()

def input_with_default(msg, default):
  i = raw_input("%s [%s]: " % (msg, default))
  if i.strip() == "":
    i = default
  return i

def handle_release_virgin():
  """This sounds dirty!"""
  print """
    It appears that this is the first time you are using this script.  I need to ask you a few questions before
    we can proceed.  Default values are in brackets, just hitting ENTER will accept the default value.
    
    Lets get started!
    """
  s = {}  
  s["svn_base"] = input_with_default("Base Subversion URL to use", "https://svn.jboss.org/repos/infinispan") 
  s["local_tags_dir"] = input_with_default("Local tags directory to use", "%s/Code/infinispan/tags" % os.getenv("HOME"))
  s["multi_threaded"] = input_with_default("Enable multithreaded (EXPERIMENTAL!)", "False")
  
  f = open(settings_file, "w")
  try:
    for e in s.keys():
      f.write("  %s = %s \n" % (e, s[e]))
  finally:
    f.close()
    
def require_settings_file(recursive = False):
  """Tests whether the settings file exists, and if not exits with an error message."""
  f = None
  try:
    f = open(settings_file)
  except IOError as ioe:
    if not recursive:
      handle_release_virgin()
      require_settings_file(True)
    else:
      print "User-specific environment settings file %s is missing!  Cannot proceed!" % settings_file
      print "Please create a file called %s with the following lines:" % settings_file
      print '''
       svn_base = https://svn.jboss.org/repos/infinispan
       local_tags_dir = /PATH/TO/infinispan/tags
       local_mvn_repo_dir = /PATH/TO/maven2/org/infinispan
       multi_threaded = False
      '''
      sys.exit(3)
  finally:
    if f:
      f.close()

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

def is_in_svn(directory):
  return os.path.isdir(directory + "/.svn")

def maven_build_distribution():
  """Builds the distribution in the current working dir"""
  subprocess.check_call(["mvn", "install", "-Pjmxdoc", "-Dmaven.test.skip.exec=true"])
  subprocess.check_call(["mvn", "install", "-Pconfigdoc", "-Dmaven.test.skip.exec=true"])
  subprocess.check_call(["mvn", "deploy", "-Pdistribution", "-Dmaven.test.skip.exec=true"])

def get_version_pattern(): 
  return re.compile("^([4-9]\.[0-9])\.[0-9]\.(Final|(ALPHA|BETA|CR)[1-9][0-9]?)$", re.IGNORECASE)

def get_version_major_minor(full_version):
  pattern = get_version_pattern()
  matcher = pattern.match(full_version)
  return matcher.group(1)

def assert_python_minimum_version(major, minor):
  e = re.compile('([0-9])\.([0-9])\.([0-9]).*')
  m = e.match(sys.version)
  major_ok = int(m.group(1)) == major
  minor_ok = int(m.group(2)) >= minor
  if not (minor_ok and major_ok):
    print "This script requires Python >= %s.%s.0.  You have %s" % (major, minor, sys.version)
    sys.exit(3)
  
    
  

  
