package org.infinispan.query.mapper.mapping.impl;

import java.io.InputStream;
import java.net.URL;
import java.util.ServiceLoader;

import org.hibernate.search.engine.environment.classpath.spi.ClassResolver;
import org.hibernate.search.engine.environment.classpath.spi.ResourceResolver;
import org.hibernate.search.engine.environment.classpath.spi.ServiceResolver;
import org.infinispan.query.mapper.log.impl.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * An implementation of the {@link ClassResolver}, {@link ResourceResolver} and {@link ServiceResolver}
 * contracts that just delegates loading to an actual {@link ClassLoader}.
 *
 * @author anistor@redhat.com
 * @since 9.2
 */
public final class ClassLoaderServiceImpl implements ClassResolver, ResourceResolver, ServiceResolver {

   private static final Log log = LogFactory.getLog(ClassLoaderServiceImpl.class, Log.class);

   private final ClassLoader classLoader;

   public ClassLoaderServiceImpl(ClassLoader classLoader) {
      this.classLoader = classLoader;
   }

   @Override
   public Class<?> classForName(String className) {
      try {
         return Class.forName(className, true, classLoader);
      } catch (Exception | LinkageError e) {
         throw log.unableToLoadTheClass( className, e );
      }
   }

   @Override
   public Package packageForName(String packageName) {
      try {
         return Class.forName(packageName + ".package-info", true, classLoader)
                 .getPackage();
      } catch (Exception | LinkageError e) {
         return null;
      }
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
   public URL locateResource(String resourceName) {
      try {
         return classLoader.getResource(resourceName);
      } catch (Exception e) {
         return null;
      }
   }

   @Override
   public <S> Iterable<S> loadJavaServices(Class<S> serviceContract) {
      return ServiceLoader.load(serviceContract, classLoader);
   }
}
