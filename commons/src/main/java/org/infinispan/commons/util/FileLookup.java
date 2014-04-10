package org.infinispan.commons.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Collection;
import java.util.HashSet;

import org.infinispan.commons.logging.BasicLogFactory;
import org.jboss.logging.BasicLogger;

public class FileLookup {

   private static final BasicLogger log = BasicLogFactory.getLog(FileLookup.class);

   /**
    * Looks up the file, see : {@link DefaultFileLookup}.
    *
    * @param filename might be the name of the file (too look it up in the class path) or an url to a file.
    * @return an input stream to the file or null if nothing found through all lookup steps.
    */
   public InputStream lookupFile(String filename, ClassLoader cl) {
      try {
         return lookupFileStrict( filename, cl );
      }
      catch (FileNotFoundException e) {
         return null;
      }
   }

   /**
    * Looks up the file, see : {@link DefaultFileLookup}.
    *
    * @param filename might be the name of the file (too look it up in the class path) or an url to a file.
    * @return an input stream to the file or null if nothing found through all lookup steps.
    * @throws FileNotFoundException if file cannot be found
    */
   public InputStream lookupFileStrict(String filename, ClassLoader cl) throws FileNotFoundException {
      InputStream is = filename == null || filename.length() == 0 ? null : getAsInputStreamFromClassLoader(filename, cl);
      if (is == null) {
         if (log.isDebugEnabled())
            log.debugf("Unable to find file %s in classpath; searching for this file on the filesystem instead.", filename);
         return new FileInputStream(filename);
      }
      return is;
   }

   public InputStream lookupFileStrict(URI uri, ClassLoader cl) throws FileNotFoundException {
      return new FileInputStream(new File(uri));
   }

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
   
   private InputStream getAsInputStreamFromClassLoader(String filename, ClassLoader appClassLoader) {
      for (ClassLoader cl : Util.getClassLoaders(appClassLoader))  {
         if (cl == null)
            continue;
         try {
            InputStream is = cl.getResourceAsStream(filename);
            if (is != null) {
               return is;
            }
         } catch (RuntimeException e) {
            // Ignore this as the classloader may throw exceptions for a valid path on Windows
         }
      }
      return null;
   }
   
   private URL getAsURLFromClassLoader(String filename, ClassLoader userClassLoader) {
      for (ClassLoader cl : Util.getClassLoaders(userClassLoader))  {
         if (cl == null)
            continue;

         try {
            URL url = cl.getResource(filename);
            if (url != null) {
               return url;
            }
         } catch (RuntimeException e) {
            // Ignore this as the classloader may throw exceptions for a valid path on Windows
         }
      }
      return null;
   }
   
   private Collection<URL> getAsURLsFromClassLoader(String filename, ClassLoader userClassLoader) throws IOException {
      Collection<URL> urls = new HashSet<URL>(4);
      for (ClassLoader cl : Util.getClassLoaders(userClassLoader))  {
         if (cl == null)
            continue;
         try {
            urls.addAll(new EnumerationList<URL>(cl.getResources(filename)));
         } catch (RuntimeException e) {
            // Ignore this as the classloader may throw exceptions for a valid path on Windows
         }
      }
      return urls;
   }

}