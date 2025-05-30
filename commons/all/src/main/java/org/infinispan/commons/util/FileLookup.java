package org.infinispan.commons.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.util.Collection;

public interface FileLookup {

   /**
    * Looks up the file, see : {@link FileLookupFactory.DefaultFileLookup}.
    *
    * @param filename might be the name of the file (too look it up in the class path) or an url to a file.
    * @return an input stream to the file or null if nothing found through all lookup steps.
    */
   InputStream lookupFile(String filename, ClassLoader cl);

   /**
    * Looks up the file, see : {@link FileLookupFactory.DefaultFileLookup}.
    *
    * @param filename might be the name of the file (to look it up in the class path) or an url to a file.
    * @return an input stream to the file or null if nothing found through all lookup steps.
    * @throws FileNotFoundException if file cannot be found
    */
   InputStream lookupFileStrict(String filename, ClassLoader cl) throws FileNotFoundException;

   /**
    * Looks up the file, see : {@link FileLookupFactory.DefaultFileLookup}.
    *
    *
    * @param uri An absolute, hierarchical URI with a scheme equal to
    *         <code>"file"</code> that represents the file to lookup
    * @return an input stream to the file or null if nothing found through all lookup steps.
    * @throws FileNotFoundException if file cannot be found
    */
   InputStream lookupFileStrict(URI uri, ClassLoader cl) throws FileNotFoundException;

   /**
    * Looks up the file and returns its URL
    * @param filename
    * @param cl
    * @return the URL pointing to the file, null if it cannot be found
    */
   URL lookupFileLocation(String filename, ClassLoader cl);

   /**
    * Same as {@link #lookupFileLocation(String, ClassLoader)} but throws a {@link FileNotFoundException} if the file
    * cannot be found.
    * @param filename
    * @param cl
    * @return
    */
   URL lookupFileLocationStrict(String filename, ClassLoader cl) throws FileNotFoundException;

   Collection<URL> lookupFileLocations(String filename, ClassLoader cl) throws IOException;

}
