package org.infinispan.commons.util;

import static org.infinispan.commons.logging.Log.CONTAINER;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Collection;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.FileLookupFactory.DefaultFileLookup;

public abstract class AbstractFileLookup implements FileLookup {

   private static final Log log = LogFactory.getLog(AbstractFileLookup.class);

   public AbstractFileLookup() {
      super();
   }

   /**
    * Looks up the file, see : {@link DefaultFileLookup}.
    *
    * @param filename might be the name of the file (too look it up in the class path) or an url to a file.
    * @return an input stream to the file or null if nothing found through all lookup steps.
    */
   @Override
   public InputStream lookupFile(String filename, ClassLoader cl) {
      try {
         return lookupFileStrict( filename, cl );
      }
      catch (FileNotFoundException e) {
         return null;
      }
   }

   protected abstract InputStream getAsInputStreamFromClassLoader(String filename, ClassLoader cl);

   /**
    * Looks up the file, see : {@link DefaultFileLookup}.
    *
    * @param filename might be the name of the file (too look it up in the class path) or an url to a file.
    * @return an input stream to the file or null if nothing found through all lookup steps.
    * @throws FileNotFoundException if file cannot be found
    */
   @Override
   public InputStream lookupFileStrict(String filename, ClassLoader cl) throws FileNotFoundException {
      InputStream is = filename == null || filename.length() == 0 ? null : getAsInputStreamFromClassLoader(filename, cl);
      if (is == null) {
         if (log.isDebugEnabled())
            log.debugf("Unable to find file %s in classpath; searching for this file on the filesystem instead.", filename);
         return new FileInputStream(filename);
      }
      return is;
   }

   @Override
   public InputStream lookupFileStrict(URI uri, ClassLoader cl) throws FileNotFoundException {
      String scheme = uri.getScheme();
      switch (scheme) {
         case "file":
            return new FileInputStream(new File(uri.getPath()));
         case "jar": {
            String uriAsString = uri.toString();
            String insideJarFilePath = uriAsString.substring(uriAsString.lastIndexOf("!") + 1);

            InputStream streamToBeReturned = getAsInputStreamFromClassLoader(insideJarFilePath, cl);
            if (streamToBeReturned == null) {
               throw CONTAINER.unableToLoadFileUsingScheme(scheme);
            }
            return streamToBeReturned;
         }
         default:
            InputStream streamToBeReturned = getAsInputStreamFromClassLoader(uri.toString(), cl);
            if(streamToBeReturned == null) {
               throw CONTAINER.unableToLoadFileUsingScheme(scheme);
            }
            return streamToBeReturned;
      }
   }

   @Override
   public URL lookupFileLocation(String filename, ClassLoader cl) {
      URL u = getAsURLFromClassLoader(filename, cl);

      if (u == null) {
         File f = new File(filename);
         if (f.exists()) try {
            u = f.toURI().toURL();
         }
         catch (MalformedURLException e) {
            // what do we do here?
         }
      }
      return u;
   }

   protected abstract URL getAsURLFromClassLoader(String filename, ClassLoader cl);

   @Override
   public Collection<URL> lookupFileLocations(String filename, ClassLoader cl) throws IOException {
      Collection<URL> u = getAsURLsFromClassLoader(filename, cl);

      File f = new File(filename);
      if (f.exists()) try {
         u.add(f.toURI().toURL());
      }
      catch (MalformedURLException e) {
         // what do we do here?
      }
      return u;
   }

   protected abstract Collection<URL> getAsURLsFromClassLoader(String filename, ClassLoader cl) throws IOException;

}
