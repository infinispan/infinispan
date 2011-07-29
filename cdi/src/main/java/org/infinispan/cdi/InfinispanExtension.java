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
import org.infinispan.cdi.interceptor.CacheRemoveAllInterceptor;
import org.infinispan.cdi.interceptor.CacheRemoveEntryInterceptor;
import org.infinispan.cdi.interceptor.CacheResultInterceptor;
import org.infinispan.cdi.interceptor.literal.CacheRemoveAllLiteral;
import org.infinispan.cdi.interceptor.literal.CacheRemoveEntryLiteral;
import org.infinispan.cdi.interceptor.literal.CacheResultLiteral;
import org.infinispan.config.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.jboss.seam.solder.bean.Beans;
import org.jboss.seam.solder.reflection.annotated.AnnotatedTypeBuilder;

import javax.cache.interceptor.CacheRemoveAll;
import javax.cache.interceptor.CacheRemoveEntry;
import javax.cache.interceptor.CacheResult;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.Any;
import javax.enterprise.inject.Default;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.spi.AfterDeploymentValidation;
import javax.enterprise.inject.spi.AnnotatedMember;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.BeforeBeanDiscovery;
import javax.enterprise.inject.spi.Extension;
import javax.enterprise.inject.spi.ProcessAnnotatedType;
import javax.enterprise.inject.spi.ProcessProducer;
import javax.enterprise.inject.spi.Producer;
import javax.enterprise.util.AnnotationLiteral;
import java.lang.annotation.Annotation;
import java.util.HashSet;
import java.util.Set;

/**
 * The Infinispan CDI extension class.
 *
 * @author Pete Muir
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
@ApplicationScoped
public class InfinispanExtension implements Extension {

   private final Set<ConfigurationHolder> configurations;

   InfinispanExtension() {
      this.configurations = new HashSet<InfinispanExtension.ConfigurationHolder>();
   }

   void registerInterceptorBindings(@Observes BeforeBeanDiscovery event) {
      event.addInterceptorBinding(CacheResult.class);
      event.addInterceptorBinding(CacheRemoveEntry.class);
      event.addInterceptorBinding(CacheRemoveAll.class);
   }

   void registerCacheResultInterceptor(@Observes ProcessAnnotatedType<CacheResultInterceptor> event) {
      event.setAnnotatedType(new AnnotatedTypeBuilder<CacheResultInterceptor>()
                                   .readFromType(event.getAnnotatedType())
                                   .addToClass(CacheResultLiteral.INSTANCE)
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

   void saveCacheConfigurations(@Observes ProcessProducer<?, Configuration> event, BeanManager beanManager) {
      Infinispan annotation = event.getAnnotatedMember().getAnnotation(Infinispan.class);
      if (annotation != null) {
         String name = annotation.value();
         configurations.add(new ConfigurationHolder(
               event.getProducer(),
               name,
               event.getAnnotatedMember(),
               beanManager
         ));
      }
   }

   void registerCacheConfigurations(@Observes AfterDeploymentValidation event, CacheManagerEventBridge eventBridge, @Any Instance<EmbeddedCacheManager> cacheContainers, BeanManager beanManager) {
      CreationalContext<Configuration> ctx = beanManager.createCreationalContext(null);
      Instance<EmbeddedCacheManager> defaultCacheManager = cacheContainers.select(new AnnotationLiteral<Default>() {});

      for (ConfigurationHolder oneConfigurationHolder : configurations) {
         String cacheName = oneConfigurationHolder.getName();
         Configuration configuration = oneConfigurationHolder.getProducer().produce(ctx);
         Set<Annotation> qualifiers = oneConfigurationHolder.getQualifiers();

         Instance<EmbeddedCacheManager> cacheManager = cacheContainers.select(qualifiers.toArray(new Annotation[qualifiers.size()]));
         if (cacheManager.isUnsatisfied()) {
            cacheManager = defaultCacheManager;
         }

         // if the cache name is empty this is the default configuration. This configuration is registered
         // by default in the default cache manager.
         if (!cacheName.isEmpty() && configuration != null) {
            cacheManager.get().defineConfiguration(cacheName, configuration);
         }

         // register cache manager observers
         eventBridge.registerObservers(qualifiers, cacheName, cacheManager.get());
      }
   }

   static class ConfigurationHolder {
      private final Producer<Configuration> producer;
      private final Set<Annotation> qualifiers;
      private final String name;

      ConfigurationHolder(Producer<Configuration> producer, String name, AnnotatedMember<?> annotatedMember, BeanManager beanManager) {
         this.producer = producer;
         this.name = name;
         this.qualifiers = Beans.getQualifiers(beanManager, annotatedMember.getAnnotations());
      }

      Producer<Configuration> getProducer() {
         return producer;
      }

      String getName() {
         return name;
      }

      public Set<Annotation> getQualifiers() {
         return qualifiers;
      }
   }
}
