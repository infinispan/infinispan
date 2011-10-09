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

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.cdi.event.cache.CacheEventBridge;
import org.infinispan.manager.CacheContainer;
import org.jboss.solder.bean.generic.ApplyScope;
import org.jboss.solder.bean.generic.Generic;
import org.jboss.solder.bean.generic.GenericConfiguration;

import javax.enterprise.inject.Instance;
import javax.enterprise.inject.Produces;
import javax.enterprise.inject.spi.AnnotatedMember;
import javax.enterprise.inject.spi.BeanManager;
import javax.inject.Inject;

import static org.jboss.solder.bean.Beans.getQualifiers;

/**
 * This class is responsible to produce the {@link Cache} and {@link AdvancedCache}. This class use the
 * <a href="http://docs.jboss.org/seam/3/solder/latest/reference/en-US/html_single/#genericbeans">Generic Beans</a>
 * mechanism provided by Seam Solder.
 *
 * @author Pete Muir
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
@GenericConfiguration(ConfigureCache.class)
public class CacheManager {

   @Inject
   private CacheContainer defaultCacheContainer;

   @Inject
   @Generic
   private Instance<CacheContainer> cacheContainer;

   @Inject
   @Generic
   private ConfigureCache configureCache;

   @Inject
   @Generic
   private AnnotatedMember<?> annotatedMember;

   @Inject
   private CacheEventBridge cacheEventBridge;

   private CacheContainer getCacheContainer() {
      if (cacheContainer.isUnsatisfied()) {
         return defaultCacheContainer;
      } else {
         return cacheContainer.get();
      }
   }

   @Produces
   @ApplyScope
   public <K, V> AdvancedCache<K, V> getAdvancedCache(BeanManager beanManager) {
      final String name = configureCache.value();
      Cache<K, V> cache;

      if (name.isEmpty()) {
         cache = getCacheContainer().getCache();
      } else {
         cache = getCacheContainer().getCache(name);
      }

      cacheEventBridge.registerObservers(
            getQualifiers(beanManager, annotatedMember.getAnnotations()),
            cache
      );

      return cache.getAdvancedCache();
   }
}
