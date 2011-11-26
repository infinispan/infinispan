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

import javax.enterprise.inject.Produces;
import javax.enterprise.inject.spi.InjectionPoint;
import javax.inject.Inject;
import java.lang.annotation.Annotation;

/**
 * The remote cache producer.
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public class RemoteCacheProducer {

   @Inject
   private RemoteCacheManager defaultRemoteCacheManager;

   @Remote
   @Produces
   public <K, V> RemoteCache<K, V> getRemoteCache(InjectionPoint injectionPoint) {
      for (Annotation qualifier : injectionPoint.getQualifiers()) {
         if (qualifier.annotationType().equals(Remote.class)) {
            Remote remote = (Remote) qualifier;
            if (!remote.value().isEmpty()) {
               return defaultRemoteCacheManager.getCache(remote.value());
            }
         }
      }

      return defaultRemoteCacheManager.getCache();
   }
}
