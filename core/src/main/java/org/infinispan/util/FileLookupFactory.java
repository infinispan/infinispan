package org.infinispan.util;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collection;
import java.util.HashSet;

import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleReference;


public class FileLookupFactory {

   public static FileLookup newInstance() {
      ClassLoader cl = FileLookup.class.getClassLoader();
      if (cl.getClass().getName().equals("org.osgi.framework.BundleReference"))
         return new OsgiFileLookup();
      else
         return new DefaultFileLookup();
      
   }
   
   public static class OsgiFileLookup extends DefaultFileLookup {
      
      private OsgiFileLookup() {
      }
      
      protected Collection<URL> getAsURLsFromClassLoader(String filename, ClassLoader userClassLoader) throws IOException {    
         Collection<URL> urls = super.getAsURLsFromClassLoader(filename, userClassLoader);
         // scan osgi bundles
         BundleContext bc = ((BundleReference) FileLookup.class.getClassLoader()).getBundle().getBundleContext();
         for (Bundle bundle : bc.getBundles()) {
            urls.add(bundle.getResource(filename));
         }
         return urls;
      }

   }

   
   public static class DefaultFileLookup extends AbstractFileLookup implements FileLookup {

      private DefaultFileLookup() {
      }
      
      protected InputStream getAsInputStreamFromClassLoader(String filename, ClassLoader appClassLoader) {
         for (ClassLoader cl : Util.getClassLoaders(appClassLoader))  {
            if (cl == null)
               continue;
            try {
               return cl.getResourceAsStream(filename);
            } catch (RuntimeException e) {
               // Ignore this as the classloader may throw exceptions for a valid path on Windows
            }
         }
         return null;
      }
      
      protected URL getAsURLFromClassLoader(String filename, ClassLoader userClassLoader) {
         for (ClassLoader cl : Util.getClassLoaders(userClassLoader))  {
            if (cl == null)
               continue;

            try {
               return cl.getResource(filename);
            } catch (RuntimeException e) {
               // Ignore this as the classloader may throw exceptions for a valid path on Windows
            }
         }
         return null;
      }
      
      protected Collection<URL> getAsURLsFromClassLoader(String filename, ClassLoader userClassLoader) throws IOException {    
         Collection<URL> urls = new HashSet<URL>();
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

   
}
