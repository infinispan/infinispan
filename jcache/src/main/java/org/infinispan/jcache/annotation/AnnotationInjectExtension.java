/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.jcache.annotation;

import org.infinispan.jcache.annotation.solder.AnnotatedTypeBuilder;

import javax.cache.annotation.CachePut;
import javax.cache.annotation.CacheRemoveAll;
import javax.cache.annotation.CacheRemoveEntry;
import javax.cache.annotation.CacheResult;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.spi.BeforeBeanDiscovery;
import javax.enterprise.inject.spi.Extension;
import javax.enterprise.inject.spi.ProcessAnnotatedType;

/**
 * CDI extension to allow injection of of cache values based on annotations
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public class AnnotationInjectExtension implements Extension {

   void registerInterceptorBindings(@Observes BeforeBeanDiscovery event) {
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

}
