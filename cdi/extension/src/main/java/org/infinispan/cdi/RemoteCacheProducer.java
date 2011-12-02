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

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;

import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.Produces;
import javax.enterprise.inject.spi.Annotated;
import javax.enterprise.inject.spi.InjectionPoint;
import javax.inject.Inject;
import java.lang.annotation.Annotation;
import java.util.Set;

import static org.jboss.solder.reflection.AnnotationInspector.getMetaAnnotation;

/**
 * The {@link RemoteCache} producer.
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
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
