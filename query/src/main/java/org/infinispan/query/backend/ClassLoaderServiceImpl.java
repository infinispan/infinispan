package org.infinispan.query.backend;

import java.io.InputStream;
import java.net.URL;
import java.util.ServiceLoader;

import org.hibernate.search.engine.service.classloading.spi.ClassLoaderService;
import org.hibernate.search.engine.service.classloading.spi.ClassLoadingException;

/**
 * An implementation of the {@link ClassLoaderService} contract that just delegates loading to an actual {@link
 * ClassLoader}.
 *
 * @author anistor@redhat.com
 * @since 9.2
 */
final class ClassLoaderServiceImpl implements ClassLoaderService {

   private final ClassLoader classLoader;

   ClassLoaderServiceImpl(ClassLoader classLoader) {
      this.classLoader = classLoader;
   }

   @Override
   @SuppressWarnings("unchecked")
   public <T> Class<T> classForName(String className) {
      try {
         return (Class<T>) Class.forName(className, true, classLoader);
      } catch (Exception e) {
         throw new ClassLoadingException("Unable to load class [" + className + "]", e);
      }
   }

   @Override
   public URL locateResource(String name) {
      try {
         return classLoader.getResource(name);
      } catch (Exception e) {
         // ignored
      }
      return null;
   }

   @Override
   public InputStream locateResourceStream(String name) {
      try {
         InputStream is = classLoader.getResourceAsStream(name);
         if (is != null) {
            return is;
         }
      } catch (Exception e) {
         // ignored
      }

      if (name.startsWith("/")) {
         String stripped = name.substring(1);
         try {
            return new URL(stripped).openStream();
         } catch (Exception e) {
            // ignored
         }

         try {
            return classLoader.getResourceAsStream(stripped);
         } catch (Exception e) {
            // ignored
         }
      }

      return null;
   }

   @Override
   public <S> Iterable<S> loadJavaServices(Class<S> serviceContract) {
      return ServiceLoader.load(serviceContract, classLoader);
   }
}
