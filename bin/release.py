#!/usr/bin/python
import re
import sys
import os
import subprocess
import shutil

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

### Globals
#  CONFIGURABLE VARIABLES

# Base SVN directory for this release.  There should be a "tags" and "trunk" directory under this.
svnBase="https://svn.jboss.org/repos/infinispan"

# Where do you locally check out tags?
localTagsDir="/Users/manik/Code/infinispan/tags"

# Your maven2 repo to deploy built artifacts
localMvnRepoDir="/Users/manik/Code/maven2/org/infinispan"

################################################################################
#
# Helper functions
#
################################################################################
maven_pom_xml_namespace = "http://maven.apache.org/POM/4.0.0"

modules = []
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

            $ bin/release.py <version>

        E.g.,

            $ bin/release.py 4.1.1.BETA1

        Please ensure you have edited bin/release.py to suit your ennvironment.
        There are configurable variables at the start of this file that is
        specific to your environment.
    '''
    sys.exit(0)

def validateVersion(version):
  versionPattern = re.compile("^[4-9]\.[0-9]\.[0-9]\.(GA|(ALPHA|BETA|CR|SP)[1-9][0-9]?)$", re.IGNORECASE)
  if versionPattern.match(version):
    return version.strip().upper()
  else:
    print "Invalid version '"+version+"'!\n"
    helpAndExit()

def tagInSubversion(version, newVersion):
  sc = get_svn_conn()
  sc.tag("%s/trunk" % svnBase, newVersion, version)

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
  print " ... updated %s" % pomFile

def patch(pomFile, version):
  ## Updates the version in a POM file
  ## We need to locate //project/parent/version, //project/version and //project/properties/project-version
  ## And replace the contents of these with the new version
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
      print "%s is %s.  Setting to %s" % (str(tag), tag.text, version)
      tag.text=version
      need_to_write = True
    
  if need_to_write:
    # write to file again!
    writePom(tree, pomFile)
  else:
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
 
def updateVersions(version, workingDir, trunkDir, test = False):
  if test:
    shutil.copytree(trunkDir, workingDir)
  else:
    client = get_svn_conn()
    client.checkout(svnBase + "/tags/" + version, workingDir)
    
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
  
  if not test:
    # Now make sure this goes back into SVN.
    checkInMessage = "Infinispan Release Script: Updated version numbers"
    client.checkin(workingDir, checkInMessage)
  
def buildAndTest(workingDir):
  os.chdir(workingDir)
  maven_build_distribution()

def getModuleName(pomFile):
  tree = ElementTree()
  tree.parse(pomFile)
  return tree.findtext("./{%s}artifactId" % maven_pom_xml_namespace)

def checkInMaven2Repo(version, workingDir):
  os.chdir(localMvnRepoDir)
  client = get_svn_conn()
  poms = [workingDir + "/pom.xml"]
  for m in modules:
    poms.append(workingDir + "/" + m + "/pom.xml")
  moduleNames=[]
  for p in poms:
    moduleNames.append(localMvnRepoDir + "/" + getModuleName(p) + "/" + version)
  client.add(moduleNames)
  for mn in moduleNames:
    checkInMessage = "Infinispan Release Script: Releasing module " + mn + " version " + version + " to public Maven2 repo"
    client.checkin(mn, checkInMessage)

def uploadArtifactsToSourceforge(version):
  os.mkdir(".tmp")
  os.mkdir(".tmp/%s" % version)
  os.chdir(".tmp")
  shutil.copy("%s/infinispan/%s/*.zip" % (localMvnRepoDir, version), "%s/" % version)
  subprocess.check_call(["scp", "-r", version, "sourceforge_frs:/home/frs/project/i/in/infinispan/infinispan"])

def uploadJavadocs(base_dir, workingDir, version):
  os.chdir("%s/target/distribution" % workingDir)
  ## Grab the distribution archive and un-arch it
  subprocess.check_call(["unzip", "infinispan-%s-all.zip" % version])
  os.chdir("infinispan-%s/doc" % version)
  ## "Fix" the docs to use the appropriate analytics tracker ID
  subprocess.check_call(["%s/bin/updateTracker.sh" % workingDir])
  subprocess.check_call(["tar", "zcf", "%s/apidocs-%s.tar.gz" % (base_dir, version), "apidocs"])
  ## Upload to sourceforge
  os.chdir(base_dir)
  subprocess.check_call(["scp", "apidocs-%s.tar.gz" % version, "sourceforge_frs:"])
  print "API docs are in %s/apidocs-%s.tar.gz" % (base_dir, version)
  print "They have also been uploaded to Sourceforge."
  print "MANUAL STEPS:"
  print "  1) Email archive to helpdesk@redhat.com"
  print "  2) SSH to sourceforge and run apidocs.sh"
  print ""    
  
### This is the starting place for this script.
def release():
  # We start by determining whether the version passed in is a valid one
  if len(sys.argv) < 2:
    helpAndExit()
  base_dir = os.getcwd()
  version = validateVersion(sys.argv[1])
  print "Releasing Infinispan version " + version
  print "Please stand by!"
  
  ## Release order:
  # Step 1: Tag in SVN
  newVersion = "%s/tags/%s" % (svnBase, version)
  print "Step 1: Tagging trunk in SVN as %s" % newVersion    
  tagInSubversion(version, newVersion)
  print "Step 1: Complete"
  
  workingDir = localTagsDir + "/" + version
    
  # Step 2: Update version in tagged files
  print "Step 2: Updating version number in source files"
  updateVersions(version, workingDir, base_dir)
  print "Step 2: Complete"
  
  # Step 3: Build and test in Maven2
  print "Step 3: Build and test in Maven2"
  buildAndTest(workingDir)
  print "Step 3: Complete"
  
  # Step 4: Check in to Maven2 repo
  print "Step 4: Checking in to Maven2 Repo (this can take a while, go get coffee)"
  checkInMaven2Repo(version, workingDir)
  print "Step 4: Complete"
  
  # Step 5: Upload javadocs to FTP
  print "Step 5: Uploading Javadocs"
  uploadJavadocs(base_dir, workingDir, version)
  print "Step 5: Complete"
  
  print "Step 6: Uploading to Sourceforge"
  uploadArtifactsToSourceforge(version)
  print "Step 6: Complete"
  
  # (future)
  # Step 6: Update www.infinispan.org
  # Step 7; Upload to SF.net
  
  print "\n\n\nDone!  Now all you need to do is:"
  print "   1.  Update http://www.infinispan.org"
  print "   2.  Update wiki pages with relevant information and links to docs, etc"
  print "   3.  Login to the Sourceforge project admin page and mark the -bin.ZIP package as the default download for all platforms."


if __name__ == "__main__":
    release()
