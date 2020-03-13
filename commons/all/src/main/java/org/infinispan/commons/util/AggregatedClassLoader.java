package org.infinispan.commons.util;

import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * A ClassLoader that delegates loading of classes and resources to a list of delegate ClassLoaders. The loading is
 * attempted in the order returned by the provided {@link Collection}.
 *
 * @author anistor@redhat.com
 * @since 9.2
 */
public final class AggregatedClassLoader extends ClassLoader {

   private final ClassLoader[] classLoaders;

   /**
    * Create an aggregated ClassLoader from a Collection of ClassLoaders
    *
    * @param classLoaders a non-empty Collection of ClassLoaders
    */
   public AggregatedClassLoader(Collection<ClassLoader> classLoaders) {
      super(null);
      if (classLoaders == null || classLoaders.isEmpty()) {
         throw new IllegalArgumentException("classLoaders argument cannot be null or empty");
      }
      this.classLoaders = classLoaders.toArray(new ClassLoader[classLoaders.size()]);
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
