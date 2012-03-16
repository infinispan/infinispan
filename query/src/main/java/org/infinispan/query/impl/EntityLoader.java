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

package org.infinispan.query.impl;

import java.util.ArrayList;
import java.util.List;

import org.hibernate.search.query.engine.spi.EntityInfo;
import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.query.backend.KeyTransformationHandler;

/**
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 * @author Marko Luksa
 * @since 5.0
 */
public class EntityLoader {
   
   private final AdvancedCache<?, ?> cache;
   private final KeyTransformationHandler keyTransformationHandler;
   
   public EntityLoader(Cache<?, ?> cache, KeyTransformationHandler keyTransformationHandler) {
      this.keyTransformationHandler = keyTransformationHandler;
      this.cache = cache.getAdvancedCache();
   }

   public Object load(EntityInfo entityInfo) {
      Object cacheKey = keyTransformationHandler.stringToKey(entityInfo.getId().toString(), cache.getClassLoader());
      return cache.get(cacheKey);
   }

   public List<Object> load(EntityInfo... entityInfos) {
      int size = entityInfos.length;
      ArrayList<Object> list = new ArrayList<Object>(size);
      for (EntityInfo e : entityInfos) {
         list.add(load(e));
      }
      return list;
   }

}
