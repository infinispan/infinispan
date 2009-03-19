Before committing anything, PLEASE make sure you have the following in your ~/.subversion/config.
This will ensure files have the *correct* line endings. Note: we do not use native since cygwin
users like LF endings. Also be sure that your IDE uses this config:

[miscellany]
enable-auto-props = yes

[auto-props]
*.java = svn:keywords=Id Revision;svn:eol-style=LF
*.xml = svn:keywords=Id Revision;svn:eol-style=LF
*.wsdl = svn:keywords=Id Revision;svn:eol-style=LF
*.xsd = svn:keywords=Id Revision;svn:eol-style=LF
*.txt = svn:keywords=Id Revision;svn:eol-style=LF
*.sh = svn:keywords=Id Revision;svn:eol-style=LF;svn:executable

