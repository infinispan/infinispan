package org.infinispan.commons.util;

import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * A ClassLoader that delegates loading of classes and resources to a list of delegate ClassLoaders.
 *
 * @author anistor@redhat.com
 * @since 9.2
 */
public class AggregatedClassLoader extends ClassLoader {

   private final ClassLoader[] classLoaders;

   public AggregatedClassLoader(ClassLoader... classLoaders) {
      super(null);
      if (classLoaders == null) {
         throw new IllegalArgumentException("classLoaders argument cannot be null");
      }
      this.classLoaders = classLoaders;
   }

   @Override
   public Enumeration<URL> getResources(String name) throws IOException {
      Set<URL> urls = new HashSet<>();
      for (ClassLoader cl : classLoaders) {
         Enumeration<URL> resources = cl.getResources(name);
         while (resources.hasMoreElements()) {
            urls.add(resources.nextElement());
         }
      }
      return new Enumeration<URL>() {
         final Iterator<URL> it = urls.iterator();

         @Override
         public boolean hasMoreElements() {
            return it.hasNext();
         }

         @Override
         public URL nextElement() {
            return it.next();
         }
      };
   }

   @Override
   protected URL findResource(String name) {
      for (ClassLoader cl : classLoaders) {
         URL res = cl.getResource(name);
         if (res != null) {
            return res;
         }
      }

      return super.findResource(name);
   }

   @Override
   protected Class<?> findClass(String name) throws ClassNotFoundException {
      for (ClassLoader cl : classLoaders) {
         try {
            return cl.loadClass(name);
         } catch (Exception ex) {
            // ignored
         }
      }

      throw new ClassNotFoundException(name);
   }
}
