package org.infinispan.jcache.annotation;

import static org.infinispan.jcache.annotation.Contracts.assertNotNull;

import java.lang.reflect.Method;
import java.util.Set;

import javax.cache.annotation.CacheDefaults;
import javax.cache.annotation.CacheKeyGenerator;
import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.jcache.logging.Log;

/**
 * An helper class providing useful methods for cache lookup.
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public final class CacheLookupHelper {

   private static final Log log = LogFactory.getLog(CacheLookupHelper.class, Log.class);

   /**
    * Disable instantiation.
    */
   private CacheLookupHelper() {
   }

   /**
    * Resolves the cache name of a method annotated with a JCACHE annotation.
    *
    * @param method                  the annotated method.
    * @param methodCacheName         the cache name defined on the JCACHE annotation.
    * @param cacheDefaultsAnnotation the {@link javax.cache.annotation.CacheDefaults} annotation instance.
    * @param generate                {@code true} if the default cache name has to be returned if none is specified.
    * @return the resolved cache name.
    * @throws NullPointerException if method or methodCacheName parameter is {@code null}.
    */
   public static String getCacheName(Method method, String methodCacheName, CacheDefaults cacheDefaultsAnnotation, boolean generate) {
      assertNotNull(method, "method parameter must not be null");
      assertNotNull(methodCacheName, "methodCacheName parameter must not be null");

      String cacheName = methodCacheName.trim();

      if (cacheName.isEmpty() && cacheDefaultsAnnotation != null) {
         cacheName = cacheDefaultsAnnotation.cacheName().trim();
      }

      if (cacheName.isEmpty() && generate) {
         cacheName = getDefaultMethodCacheName(method);
      }

      return cacheName;
   }

   /**
    * Resolves and creates an instance of {@link javax.cache.annotation.CacheKeyGenerator}. To resolve the cache key generator class the
    * algorithm defined in JCACHE specification is used.
    *
    * @param beanManager                  the bean manager instance.
    * @param methodCacheKeyGeneratorClass the {@link javax.cache.annotation.CacheKeyGenerator} class declared in the cache annotation.
    * @param cacheDefaultsAnnotation      the {@link javax.cache.annotation.CacheDefaults} annotation instance.
    * @return the {@link javax.cache.annotation.CacheKeyGenerator} instance.
    * @throws NullPointerException if beanManager parameter is {@code null}.
    */
   public static CacheKeyGenerator getCacheKeyGenerator(BeanManager beanManager, Class<? extends CacheKeyGenerator> methodCacheKeyGeneratorClass, CacheDefaults cacheDefaultsAnnotation) {
      assertNotNull(beanManager, "beanManager parameter must not be null");

      Class<? extends CacheKeyGenerator> cacheKeyGeneratorClass = DefaultCacheKeyGenerator.class;
      if (!CacheKeyGenerator.class.equals(methodCacheKeyGeneratorClass)) {
         cacheKeyGeneratorClass = methodCacheKeyGeneratorClass;
      } else if (cacheDefaultsAnnotation != null && !CacheKeyGenerator.class.equals(cacheDefaultsAnnotation.cacheKeyGenerator())) {
         cacheKeyGeneratorClass = cacheDefaultsAnnotation.cacheKeyGenerator();
      }

      final CreationalContext<?> creationalContext = beanManager.createCreationalContext(null);
      final Set<Bean<?>> beans = beanManager.getBeans(cacheKeyGeneratorClass);
      if (!beans.isEmpty()) {
         final Bean<?> bean = beanManager.resolve(beans);
         return (CacheKeyGenerator) beanManager.getReference(bean, CacheKeyGenerator.class, creationalContext);
      }

      // the CacheKeyGenerator implementation is not managed by CDI
      try {

         return cacheKeyGeneratorClass.newInstance();

      } catch (InstantiationException e) {
         throw log.unableToInstantiateCacheKeyGenerator(cacheKeyGeneratorClass, e);
      } catch (IllegalAccessException e) {
         throw log.unableToInstantiateCacheKeyGenerator(cacheKeyGeneratorClass, e);
      }
   }

   /**
    * Returns the default cache name associated to the given method according to JSR-107.
    *
    * @param method the method.
    * @return the default cache name for the given method.
    */
   private static String getDefaultMethodCacheName(Method method) {
      int i = 0;
      int nbParameters = method.getParameterTypes().length;

      StringBuilder cacheName = new StringBuilder()
            .append(method.getDeclaringClass().getName())
            .append(".")
            .append(method.getName())
            .append("(");

      for (Class<?> oneParameterType : method.getParameterTypes()) {
         cacheName.append(oneParameterType.getName());
         if (i < (nbParameters - 1)) {
            cacheName.append(",");
         }
         i++;
      }

      return cacheName.append(")").toString();
   }
}
