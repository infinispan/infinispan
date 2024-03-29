package org.infinispan.cdi.remote;

import static org.infinispan.cdi.common.util.Reflections.getMetaAnnotation;

import java.lang.annotation.Annotation;
import java.util.Set;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.enterprise.inject.spi.Annotated;
import jakarta.enterprise.inject.spi.InjectionPoint;
import jakarta.inject.Inject;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;

/**
 * The {@link RemoteCache} producer.
 *
 * @author Kevin Pollet &lt;kevin.pollet@serli.com&gt; (C) 2011 SERLI
 */
@ApplicationScoped
public class RemoteCacheProducer {

   @Inject
   private RemoteCacheManager defaultRemoteCacheManager;

   @Any
   @Inject
   private Instance<RemoteCacheManager> cacheManagers;

   /**
    * Produces the remote cache.
    *
    * @param injectionPoint the injection point.
    * @param <K>            the type of the key.
    * @param <V>            the type of the value.
    * @return the remote cache instance.
    */
   @Remote
   @Produces
   public <K, V> RemoteCache<K, V> getRemoteCache(InjectionPoint injectionPoint) {
      final Set<Annotation> qualifiers = injectionPoint.getQualifiers();
      final RemoteCacheManager cacheManager = getRemoteCacheManager(qualifiers.toArray(new Annotation[0]));
      final Remote remote = getRemoteAnnotation(injectionPoint.getAnnotated());

      if (remote != null && !remote.value().isEmpty()) {
         return cacheManager.getCache(remote.value());
      }
      return cacheManager.getCache();
   }

   /**
    * Retrieves the {@link RemoteCacheManager} bean with the following qualifiers.
    *
    * @param qualifiers the qualifiers.
    * @return the {@link RemoteCacheManager} qualified or the default one if no bean with the given qualifiers has been
    *         found.
    */
   private RemoteCacheManager getRemoteCacheManager(Annotation[] qualifiers) {
      final Instance<RemoteCacheManager> specificCacheManager = cacheManagers.select(qualifiers);

      if (specificCacheManager.isUnsatisfied()) {
         return defaultRemoteCacheManager;
      }
      return specificCacheManager.get();
   }

   /**
    * Retrieves the {@link Remote} annotation instance on the given annotated element.
    *
    * @param annotated the annotated element.
    * @return the {@link Remote} annotation instance or {@code null} if not found.
    */
   private Remote getRemoteAnnotation(Annotated annotated) {
      Remote remote = annotated.getAnnotation(Remote.class);

      if (remote == null) {
         remote = getMetaAnnotation(annotated, Remote.class);
      }
      return remote;
   }
}
