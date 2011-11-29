/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.cdi;

import org.infinispan.cdi.event.cachemanager.CacheManagerEventBridge;
import org.infinispan.cdi.interceptor.CachePutInterceptor;
import org.infinispan.cdi.interceptor.CacheRemoveAllInterceptor;
import org.infinispan.cdi.interceptor.CacheRemoveEntryInterceptor;
import org.infinispan.cdi.interceptor.CacheResultInterceptor;
import org.infinispan.cdi.interceptor.literal.CachePutLiteral;
import org.infinispan.cdi.interceptor.literal.CacheRemoveAllLiteral;
import org.infinispan.cdi.interceptor.literal.CacheRemoveEntryLiteral;
import org.infinispan.cdi.interceptor.literal.CacheResultLiteral;
import org.infinispan.cdi.util.Version;
import org.infinispan.cdi.util.logging.Log;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.config.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.util.logging.LogFactory;
import org.jboss.solder.bean.BeanBuilder;
import org.jboss.solder.bean.ContextualLifecycle;
import org.jboss.solder.reflection.annotated.AnnotatedTypeBuilder;

import javax.cache.annotation.CachePut;
import javax.cache.annotation.CacheRemoveAll;
import javax.cache.annotation.CacheRemoveEntry;
import javax.cache.annotation.CacheResult;
import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.Any;
import javax.enterprise.inject.Default;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.spi.AfterBeanDiscovery;
import javax.enterprise.inject.spi.AfterDeploymentValidation;
import javax.enterprise.inject.spi.Annotated;
import javax.enterprise.inject.spi.AnnotatedMember;
import javax.enterprise.inject.spi.AnnotatedType;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.BeforeBeanDiscovery;
import javax.enterprise.inject.spi.Extension;
import javax.enterprise.inject.spi.InjectionPoint;
import javax.enterprise.inject.spi.InjectionTarget;
import javax.enterprise.inject.spi.ProcessAnnotatedType;
import javax.enterprise.inject.spi.ProcessInjectionTarget;
import javax.enterprise.inject.spi.ProcessProducer;
import javax.enterprise.inject.spi.Producer;
import javax.enterprise.util.AnnotationLiteral;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.jboss.solder.bean.Beans.getQualifiers;
import static org.jboss.solder.reflection.Reflections.getAnnotationsWithMetaAnnotation;
import static org.jboss.solder.reflection.Reflections.getRawType;

/**
 * The Infinispan CDI extension class.
 *
 * @author Pete Muir
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public class InfinispanExtension implements Extension {

   private static final Log log = LogFactory.getLog(InfinispanExtension.class, Log.class);

   private Producer<RemoteCache> remoteCacheProducer;
   private final Set<ConfigurationHolder> configurations;
   private final Map<Type, Set<Annotation>> remoteCacheInjectionPoints;

   InfinispanExtension() {
      this.configurations = new HashSet<InfinispanExtension.ConfigurationHolder>();
      this.remoteCacheInjectionPoints = new HashMap<Type, Set<Annotation>>();
   }

   void registerInterceptorBindings(@Observes BeforeBeanDiscovery event) {
      log.version(Version.getVersion());

      event.addInterceptorBinding(CacheResult.class);
      event.addInterceptorBinding(CachePut.class);
      event.addInterceptorBinding(CacheRemoveEntry.class);
      event.addInterceptorBinding(CacheRemoveAll.class);
   }

   void registerCacheResultInterceptor(@Observes ProcessAnnotatedType<CacheResultInterceptor> event) {
      event.setAnnotatedType(new AnnotatedTypeBuilder<CacheResultInterceptor>()
                                   .readFromType(event.getAnnotatedType())
                                   .addToClass(CacheResultLiteral.INSTANCE)
                                   .create());
   }

   void registerCachePutInterceptor(@Observes ProcessAnnotatedType<CachePutInterceptor> event) {
      event.setAnnotatedType(new AnnotatedTypeBuilder<CachePutInterceptor>()
                                   .readFromType(event.getAnnotatedType())
                                   .addToClass(CachePutLiteral.INSTANCE)
                                   .create());
   }

   void registerCacheRemoveEntryInterceptor(@Observes ProcessAnnotatedType<CacheRemoveEntryInterceptor> event) {
      event.setAnnotatedType(new AnnotatedTypeBuilder<CacheRemoveEntryInterceptor>()
                                   .readFromType(event.getAnnotatedType())
                                   .addToClass(CacheRemoveEntryLiteral.INSTANCE)
                                   .create());
   }

   void registerCacheRemoveAllInterceptor(@Observes ProcessAnnotatedType<CacheRemoveAllInterceptor> event) {
      event.setAnnotatedType(new AnnotatedTypeBuilder<CacheRemoveAllInterceptor>()
                                   .readFromType(event.getAnnotatedType())
                                   .addToClass(CacheRemoveAllLiteral.INSTANCE)
                                   .create());
   }

   void saveRemoteCacheProducer(@Observes ProcessProducer<RemoteCacheProducer, RemoteCache> event) {
      remoteCacheProducer = event.getProducer();
   }

   <T> void saveRemoteInjectionPoints(@Observes ProcessInjectionTarget<T> event, BeanManager beanManager) {
      final InjectionTarget<T> injectionTarget = event.getInjectionTarget();

      for (InjectionPoint injectionPoint : injectionTarget.getInjectionPoints()) {
         final Annotated annotated = injectionPoint.getAnnotated();
         final Class<?> rawType = getRawType(annotated.getBaseType());
         final Set<Annotation> qualifiers = getQualifiers(beanManager, annotated.getAnnotations());

         if (!annotated.isAnnotationPresent(Remote.class)
               && !getAnnotationsWithMetaAnnotation(qualifiers, Remote.class).isEmpty()
               && rawType.isAssignableFrom(RemoteCache.class)) {

            Set<Annotation> current = remoteCacheInjectionPoints.get(annotated.getBaseType());
            if (current == null) {
               remoteCacheInjectionPoints.put(annotated.getBaseType(), qualifiers);
            } else {
               current.addAll(qualifiers);
            }
         }
      }
   }

   void saveCacheConfigurations(@Observes ProcessProducer<?, Configuration> event, BeanManager beanManager) {
      ConfigureCache annotation = event.getAnnotatedMember().getAnnotation(ConfigureCache.class);
      if (annotation != null) {
         String name = annotation.value();
         AnnotatedMember<?> annotatedMember = event.getAnnotatedMember();

         configurations.add(new ConfigurationHolder(
               event.getProducer(),
               name,
               getQualifiers(beanManager, annotatedMember.getAnnotations())
         ));
      }
   }

   @SuppressWarnings("unchecked")
   void registerRemoteCacheBeans(@Observes AfterBeanDiscovery event, BeanManager beanManager) {
      for (Map.Entry<Type, Set<Annotation>> entry : remoteCacheInjectionPoints.entrySet()) {
         final AnnotatedType<?> annotatedType = beanManager.createAnnotatedType(getRawType(entry.getKey()));

         event.addBean(new BeanBuilder(beanManager)
                             .readFromType(annotatedType)
                             .addType(entry.getKey())
                             .addQualifiers(entry.getValue())
                             .beanLifecycle(new ContextualLifecycle<RemoteCache>() {
                                @Override
                                public RemoteCache create(Bean<RemoteCache> bean, CreationalContext<RemoteCache> ctx) {
                                   return remoteCacheProducer.produce(ctx);
                                }

                                @Override
                                public void destroy(Bean<RemoteCache> bean, RemoteCache instance, CreationalContext<RemoteCache> ctx) {
                                   remoteCacheProducer.dispose(instance);
                                }
                             }).create());
      }
   }

   void registerCacheConfigurations(@Observes AfterDeploymentValidation event, CacheManagerEventBridge eventBridge, @Any Instance<EmbeddedCacheManager> cacheManagers, BeanManager beanManager) {
      final CreationalContext<Configuration> ctx = beanManager.createCreationalContext(null);
      final EmbeddedCacheManager defaultCacheManager = cacheManagers.select(new AnnotationLiteral<Default>() {}).get();

      for (ConfigurationHolder oneConfigurationHolder : configurations) {
         final String cacheName = oneConfigurationHolder.getName();
         final Configuration cacheConfiguration = oneConfigurationHolder.getProducer().produce(ctx);
         final Set<Annotation> cacheQualifiers = oneConfigurationHolder.getQualifiers();

         // if a specific cache manager is defined for this cache we use it
         final Instance<EmbeddedCacheManager> specificCacheManager = cacheManagers.select(cacheQualifiers.toArray(new Annotation[cacheQualifiers.size()]));
         final EmbeddedCacheManager cacheManager = specificCacheManager.isUnsatisfied() ? defaultCacheManager : specificCacheManager.get();

         // the default configuration is registered by the default cache manager producer
         if (!cacheName.trim().isEmpty()) {
            if (cacheConfiguration != null) {
               cacheManager.defineConfiguration(cacheName, cacheConfiguration);
               log.cacheConfigurationDefined(cacheName, cacheManager);
            } else if (!cacheManager.getCacheNames().contains(cacheName)) {
               cacheManager.defineConfiguration(cacheName, cacheManager.getDefaultConfiguration().clone());
               log.cacheConfigurationDefined(cacheName, cacheManager);
            }
         }

         // register cache manager observers
         eventBridge.registerObservers(cacheQualifiers, cacheName, cacheManager);
      }
   }

   static class ConfigurationHolder {
      private final Producer<Configuration> producer;
      private final Set<Annotation> qualifiers;
      private final String name;

      ConfigurationHolder(Producer<Configuration> producer, String name, Set<Annotation> qualifiers) {
         this.producer = producer;
         this.name = name;
         this.qualifiers = qualifiers;
      }

      public Producer<Configuration> getProducer() {
         return producer;
      }

      public String getName() {
         return name;
      }

      public Set<Annotation> getQualifiers() {
         return qualifiers;
      }
   }
}
