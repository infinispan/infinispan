package org.infinispan.jcache.embedded;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.net.URL;
import java.util.Enumeration;

import org.infinispan.commons.CacheException;

/**
 * A {@code ClassLoader} wrapper that only keeps a weak reference to the wrapped {@code ClassLoader}.
 *
 * <p>Useful when we don't want a cache manager to keep the classloader alive AND the classloader
 * is guaranteed to outlive the cache manager, e.g. in {@link JCachingProvider}.</p>
 *
 * @author Dan Berindei
 * @since 9.4
 */
class WeakClassLoader extends ClassLoader {
   private final WeakReference<ClassLoader> loaderRef;

   public WeakClassLoader(ClassLoader classLoader) {
      super(null);

      this.loaderRef = new WeakReference<>(classLoader);
   }

   @Override
   protected Class<?> findClass(String name) throws ClassNotFoundException {
      ClassLoader loader = requireClassLoader();
      return loader.loadClass(name);
   }

   @Override
   protected URL findResource(String name) {
      ClassLoader loader = requireClassLoader();
      return loader.getResource(name);
   }

   @Override
   protected Enumeration<URL> findResources(String name) throws IOException {
      ClassLoader loader = requireClassLoader();
      return loader.getResources(name);
   }

   private ClassLoader requireClassLoader() {
      ClassLoader loader = loaderRef.get();
      if (loader == null) {
         throw new CacheException("ClassLoader reference was garbage collected");
      }
      return loader;
   }
}
