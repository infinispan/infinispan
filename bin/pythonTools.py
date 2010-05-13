import os
import fnmatch
import re
import subprocess
import sys
import readline
import shutil
settings_file = '%s/.infinispan_dev_settings' % os.getenv('HOME')

### Known config keys
svn_base_key = "svn_base"
local_tags_dir_key = "local_tags_dir"
local_mvn_repo_dir_key = "local_mvn_repo_dir"
maven_pom_xml_namespace = "http://maven.apache.org/POM/4.0.0"
default_settings = {'dry_run': False, 'multi_threaded': False, 'verbose': False}
boolean_keys = ['dry_run', 'multi_threaded', 'verbose']

def apply_defaults(s):
  for e in default_settings.items():
    if e[0] not in s:
      s[e[0]] = e[1]
  return s

def check_mandatory_settings(s):
  missing_keys = []
  required_keys = [svn_base_key, local_tags_dir_key]
  for k in required_keys:
    if k not in s:
      missing_keys.append(k)
  
  if len(missing_keys) > 0:
    print "Entries %s are missing in configuration file %s!  Cannot proceed!" % (missing_keys, settings_file)
    sys.exit(2)

def to_bool(x):
  if type(x) == bool:
    return x
  if type(x) == str:
    return {'true': True, 'false': False}.get(x.strip().lower())  

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
    settings = apply_defaults(settings)
    check_mandatory_settings(settings)
    for k in boolean_keys:
      settings[k] = to_bool(settings[k])
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
  s["verbose"] = input_with_default("Be verbose?", False)
  s["multi_threaded"] = input_with_default("Run multi-threaded?  (Disable to debug)", True)
  s = apply_defaults(s)  
  f = open(settings_file, "w")
  try:
    for e in s.keys():
      f.write("  %s = %s \n" % (e, s[e]))
  finally:
    f.close()
    
def require_settings_file(recursive = False):
  """Tests whether the settings file exists, and if not prompts the user to create one."""
  f = None
  try:
    f = open(settings_file)
  except IOError as ioe:
    if not recursive:
      handle_release_virgin()
      require_settings_file(True)
      print "User-specific environment settings file %s created!  Please start this script again!" % settings_file
      sys.exit(4)
    else:
      print "User-specific environment settings file %s is missing!  Cannot proceed!" % settings_file
      print "Please create a file called %s with the following lines:" % settings_file
      print '''
       svn_base = https://svn.jboss.org/repos/infinispan
       local_tags_dir = /PATH/TO/infinispan/tags
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
  
  def __init__(self):
    if settings['verbose']:
      self.svn_cmd = ['svn']
    else:
      self.svn_cmd = ['svn', '-q']  
  
  def do_svn(self, params):
    commands = []
    for e in self.svn_cmd:
      commands.append(e)
    for e in params:
      commands.append(e)
    subprocess.check_call(commands)    
  
  def tag(self, fr_url, to_url, version):
    """Tags a release."""
    checkInMessage = "Infinispan Release Script: Tagging " + version
    self.do_svn(["cp", fr_url, to_url, "-m", checkInMessage])
    
  def checkout(self, url, to_dir):
    """Checks out a URL to the given directory"""
    self.do_svn(["checkout", url, to_dir])
    
  def checkin(self, working_dir, msg):
    """Checks in a working directory with the appropriate message"""
    self.do_svn(["commit", "-m", msg, working_dir])
    
  def add(self, directory):
    """Adds a directory or file to SVN.  Directory can either be the name of a file or dir, or a list of either."""
    if directory:
      call_params = ["add"]
      if isinstance(directory, str):
        call_params.append(directory)
      else:
        for d in directory:
          call_params.append(d)
      self.do_svn(call_params)      


class DryRun(object):
  location_root = "%s/%s" % (os.getenv("HOME"), "infinispan_release_dry_run")
  flags = "-r"
  
  def __init__(self):
    if settings['verbose']:
      self.flags = "-rv"
  
  def find_version(self, url):
    return os.path.split(url)[1]
      
  def copy(self, src, dst):
    print "  DryRun: Executing %s" % ['rsync', self.flags, src, dst]
    try:
      os.makedirs(dst)
    except:
      pass
    subprocess.check_call(['rsync', self.flags, src, dst])
  

class DryRunSvnConn(DryRun):
  urls = {}
  def tag(self, fr_url, to_url, version):
    self.urls[version] = '%s/svn/%s' % (self.location_root, version)
    trunk_dir = settings[local_tags_dir_key].replace('/tags', '/trunk')
    if os.path.isdir(trunk_dir) and is_in_svn(trunk_dir):
      self.copy(trunk_dir, '%s/svn' % self.location_root)
      os.rename('%s/svn/trunk' % self.location_root, self.urls[version])
    else:
      subprocess.check_call(["svn", "export", fr_url, self.urls[version]])
  
  def checkout(self, url, to_dir):
    ver = self.find_version(url)    
    if ver in self.urls:
      elems = os.path.split(to_dir)      
      self.copy(self.urls[ver], elems[0])
    else:
      subprocess.check_call(["svn", "export", url, to_dir])
  def checkin(self, working_dir, msg):
    ver = self.find_version(working_dir)
    subprocess.check_call(['rsync', working_dir, self.urls[ver]])
  def add(self, directory):
    print "  DryRunSvnConn: Adding " + directory
    pass


class Uploader(object):
  def __init__(self):
    if settings['verbose']:
      self.scp_cmd = ['scp', '-rv']
      self.rsync_cmd = ['rsync', '-rv']
    else:
      self.scp_cmd = ['scp', '-r']
      self.rsync_cmd = ['rsync', '-r']
      
  def upload_scp(self, fr, to, flags = []):
    self.upload(fr, to, flags, self.scp_cmd)
  
  def upload_rsync(self, fr, to, flags = []):
    self.upload(fr, to, flags, self.rsync_cmd)    
  
  def upload(self, fr, to, flags, cmd):
    for e in flags:
      cmd.append(e)
    cmd.append(fr)
    cmd.append(to)
    subprocess.check_call(cmd)    
  


class DryRunUploader(DryRun):
  def upload_scp(self, fr, to, flags = []):
    self.upload(fr, to, "scp")
  
  def upload_rsync(self, fr, to, flags = []):
    self.upload(fr, to.replace(':', '____').replace('@', "__"), "rsync")
  
  def upload(self, fr, to, type):
    self.copy(fr, "%s/%s/%s" % (self.location_root, type, to))    


def is_in_svn(directory):
  return os.path.isdir(directory + "/.svn")

def maven_build_distribution():
  """Builds the distribution in the current working dir"""
  mvn_commands = [["install", "-Pjmxdoc"],["install", "-Pconfigdoc"], ["deploy", "-Pdistribution"]]
    
  for c in mvn_commands:
    c.append("-Dmaven.test.skip.exec=true")
    if settings['dry_run']:
      c.append("-Dmaven.deploy.skip=true")
    if not settings['verbose']:
      c.insert(0, '-q')
    c.insert(0, 'mvn')
    subprocess.check_call(c)


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
  
    
  

  
