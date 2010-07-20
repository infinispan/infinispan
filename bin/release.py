#!/usr/bin/python
import re
import sys
import os
import subprocess
import shutil
from datetime import *
from multiprocessing import Process

try:
  from xml.etree.ElementTree import ElementTree
except:
  print '''
        Welcome to the Infinispan Release Script.
        This release script requires that you use at least Python 2.5.0.  It appears
        that you do not thave the ElementTree XML APIs available, which are available
        by default in Python 2.5.0.
        '''
  sys.exit(1)
  
from pythonTools import *

modules = []
uploader = None
svn_conn = None

def getModules(directory):
    # look at the pom.xml file
    tree = ElementTree()
    f = directory + "/pom.xml"
    print "Parsing %s to get a list of modules in project" % f
    tree.parse(f)        
    mods = tree.findall(".//{%s}module" % maven_pom_xml_namespace)
    for m in mods:
        modules.append(m.text)

def helpAndExit():
    print '''
        Welcome to the Infinispan Release Script.
        
        Usage:
        
            $ bin/release.py <version> <branch to tag from>
            
        E.g.,
        
            $ bin/release.py 4.1.1.BETA1 <-- this will tag off trunk.
            
            $ bin/release.py 4.1.1.BETA1 branches/4.1.x <-- this will use the appropriate branch
            
        Please ensure you have edited bin/release.py to suit your ennvironment.
        There are configurable variables at the start of this file that is
        specific to your environment.
    '''
    sys.exit(0)

def validateVersion(version):  
  versionPattern = get_version_pattern()
  if versionPattern.match(version):
    return version.strip().upper()
  else:
    print "Invalid version '"+version+"'!\n"
    helpAndExit()

def tagInSubversion(version, newVersion, branch):
  try:
    svn_conn.tag("%s/%s" % (settings[svn_base_key], branch), newVersion, version)
  except:
    print "FATAL: Unable to tag.  Perhaps branch %s does not exist on Subversion URL %s." % (branch, settings[svn_base_key])
    print "FATAL: Cannot continue!"
    sys.exit(200)

def getProjectVersionTag(tree):
  return tree.find("./{%s}version" % (maven_pom_xml_namespace))

def getParentVersionTag(tree):
  return tree.find("./{%s}parent/{%s}version" % (maven_pom_xml_namespace, maven_pom_xml_namespace))

def getPropertiesVersionTag(tree):
  return tree.find("./{%s}properties/{%s}project-version" % (maven_pom_xml_namespace, maven_pom_xml_namespace))

def writePom(tree, pomFile):
  tree.write("tmp.xml", 'UTF-8')
  in_f = open("tmp.xml")
  out_f = open(pomFile, "w")
  try:
    for l in in_f:
      newstr = l.replace("ns0:", "").replace(":ns0", "").replace("ns1", "xsi")
      out_f.write(newstr)
  finally:
    in_f.close()
    out_f.close()        
  if settings['verbose']:
    print " ... updated %s" % pomFile

def patch(pomFile, version):
  ## Updates the version in a POM file
  ## We need to locate //project/parent/version, //project/version and //project/properties/project-version
  ## And replace the contents of these with the new version
  if settings['verbose']:
    print "Patching %s" % pomFile
  tree = ElementTree()
  tree.parse(pomFile)    
  need_to_write = False
  
  tags = []
  tags.append(getParentVersionTag(tree))
  tags.append(getProjectVersionTag(tree))
  tags.append(getPropertiesVersionTag(tree))
  
  for tag in tags:
    if tag != None:
      if settings['verbose']:
        print "%s is %s.  Setting to %s" % (str(tag), tag.text, version)
      tag.text=version
      need_to_write = True
    
  if need_to_write:
    # write to file again!
    writePom(tree, pomFile)
  else:
    if settings['verbose']:
      print "File doesn't need updating; nothing replaced!"

def get_poms_to_patch(workingDir):
  getModules(workingDir)
  print 'Available modules are ' + str(modules)
  pomsToPatch = [workingDir + "/pom.xml"]
  for m in modules:
    pomsToPatch.append(workingDir + "/" + m + "/pom.xml")
    # Look for additional POMs that are not directly referenced!
  for additionalPom in GlobDirectoryWalker(workingDir, 'pom.xml'):
    if additionalPom not in pomsToPatch:
      pomsToPatch.append(additionalPom)
      
  return pomsToPatch

def updateVersions(version, workingDir, trunkDir):
  svn_conn.checkout(settings[svn_base_key] + "/tags/" + version, workingDir)
    
  pomsToPatch = get_poms_to_patch(workingDir)
    
  for pom in pomsToPatch:
    patch(pom, version)
    
  ## Now look for Version.java
  version_bytes = '{'
  for ch in version:
    if not ch == ".":
      version_bytes += "'%s', " % ch
  version_bytes = version_bytes[:-2]
  version_bytes += "}"
  version_java = workingDir + "/core/src/main/java/org/infinispan/Version.java"
  f_in = open(version_java)
  f_out = open(version_java+".tmp", "w")
  try:
    for l in f_in:
      if l.find("static final byte[] version_id = ") > -1:
        l = re.sub('version_id = .*;', 'version_id = ' + version_bytes + ';', l)
      else:
        if l.find("public static final String version =") > -1:
          l = re.sub('version = "[A-Z0-9\.]*";', 'version = "' + version + '";', l)
      f_out.write(l)
  finally:
    f_in.close()
    f_out.close()
    
  os.rename(version_java+".tmp", version_java)
  
  # Now make sure this goes back into SVN.
  checkInMessage = "Infinispan Release Script: Updated version numbers"
  svn_conn.checkin(workingDir, checkInMessage)

def buildAndTest(workingDir):
  os.chdir(workingDir)
  maven_build_distribution()

def getModuleName(pomFile):
  tree = ElementTree()
  tree.parse(pomFile)
  return tree.findtext("./{%s}artifactId" % maven_pom_xml_namespace)


def uploadArtifactsToSourceforge(version):
  shutil.rmtree(".tmp", ignore_errors = True)  
  os.mkdir(".tmp")
  os.mkdir(".tmp/%s" % version)
  os.chdir(".tmp")
  dist_dir = "%s/%s/target/distribution" % (settings[local_tags_dir_key], version)
  print "Copying from %s to %s" % (dist_dir, version)
  for item in os.listdir(dist_dir):
    full_name = "%s/%s" % (dist_dir, item)
    if item.strip().lower().endswith(".zip") and os.path.isfile(full_name):      
      shutil.copy2(full_name, version)
  uploader.upload_scp(version, "sourceforge_frs:/home/frs/project/i/in/infinispan/infinispan")
  shutil.rmtree(".tmp", ignore_errors = True)  

def unzip_archive(workingDir, version):
  os.chdir("%s/target/distribution" % workingDir)
  ## Grab the distribution archive and un-arch it
  shutil.rmtree("infinispan-%s" % version, ignore_errors = True)
  if settings['verbose']:
    subprocess.check_call(["unzip", "infinispan-%s-all.zip" % version])
  else:
    subprocess.check_call(["unzip", "-q", "infinispan-%s-all.zip" % version])

def uploadJavadocs(workingDir, version):
  """Javadocs get rsync'ed to filemgmt.jboss.org, in the docs_htdocs/infinispan directory"""
  version_short = get_version_major_minor(version)
  
  os.chdir("%s/target/distribution/infinispan-%s/doc" % (workingDir, version))
  ## "Fix" the docs to use the appropriate analytics tracker ID
  subprocess.check_call(["%s/bin/updateTracker.sh" % workingDir])
  os.mkdir(version_short)
  os.rename("apidocs", "%s/apidocs" % version_short)
  
  ## rsync this stuff to filemgmt.jboss.org
  uploader.upload_rsync(version_short, "infinispan@filemgmt.jboss.org:/docs_htdocs/infinispan", flags = ['-rv', '--protocol=28'])

def uploadSchema(workingDir, version):
  """Schema gets rsync'ed to filemgmt.jboss.org, in the docs_htdocs/infinispan/schemas directory"""
  os.chdir("%s/target/distribution/infinispan-%s/etc/schema" % (workingDir, version))
  
  ## rsync this stuff to filemgmt.jboss.org
  uploader.upload_rsync('.', "infinispan@filemgmt.jboss.org:/docs_htdocs/infinispan/schemas", ['-rv', '--protocol=28'])

def do_task(target, args, async_processes):
  if settings['multi_threaded']:
    async_processes.append(Process(target = target, args = args))  
  else:
    target(*args)

### This is the starting place for this script.
def release():
  global settings
  global uploader
  global svn_conn
  assert_python_minimum_version(2, 5)
  require_settings_file()
    
  # We start by determining whether the version passed in is a valid one
  if len(sys.argv) < 2:
    helpAndExit()
  
  base_dir = os.getcwd()
  version = validateVersion(sys.argv[1])
  branch = "trunk"
  if len(sys.argv) > 2:
    branch = sys.argv[2]
    
  print "Releasing Infinispan version %s from branch '%s'" % (version, branch)
  print "Please stand by!"
  
  ## Set up network interactive tools
  if settings['dry_run']:
    # Use stubs
    print "*** This is a DRY RUN.  No changes will be committed.  Used to test this release script only. ***"
    print "Your settings are %s" % settings
    uploader = DryRunUploader()
    svn_conn = DryRunSvnConn()
  else:
    uploader = Uploader()
    svn_conn= SvnConn()
  
  ## Release order:
  # Step 1: Tag in SVN
  newVersion = "%s/tags/%s" % (settings[svn_base_key], version)
  print "Step 1: Tagging trunk in SVN as %s" % newVersion    
  tagInSubversion(version, newVersion, branch)
  print "Step 1: Complete"
  
  workingDir = settings[local_tags_dir_key] + "/" + version
    
  # Step 2: Update version in tagged files
  print "Step 2: Updating version number in source files"
  updateVersions(version, workingDir, base_dir)
  print "Step 2: Complete"
  
  # Step 3: Build and test in Maven2
  print "Step 3: Build and test in Maven2"
  buildAndTest(workingDir)
  print "Step 3: Complete"
  
  async_processes = []
  
  ##Unzip the newly built archive now
  unzip_archive(workingDir, version)
    
  # Step 4: Upload javadocs to FTP
  print "Step 4: Uploading Javadocs"  
  do_task(uploadJavadocs, [workingDir, version], async_processes)
  print "Step 4: Complete"
  
  print "Step 5: Uploading to Sourceforge"
  do_task(uploadArtifactsToSourceforge, [version], async_processes)    
  print "Step 5: Complete"
  
  print "Step 6: Uploading to configuration XML schema"
  do_task(uploadSchema, [workingDir, version], async_processes)    
  print "Step 6: Complete"
  
  ## Wait for processes to finish
  for p in async_processes:
    p.start()
  
  for p in async_processes:
    p.join()
  
  print "\n\n\nDone!  Now all you need to do is the remaining post-release tasks as outlined in https://docspace.corp.redhat.com/docs/DOC-28594"

if __name__ == "__main__":
  release()
