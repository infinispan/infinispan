/* 
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
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
 * @since 5.0
 */
public class EntityLoader {
   
   private final AdvancedCache cache;
   
   public EntityLoader(Cache cache) {
      this.cache = cache.getAdvancedCache();
   }

   public Object load(EntityInfo entityInfo) {
      Object cacheKey = KeyTransformationHandler.stringToKey(entityInfo.getId().toString());
      return cache.get(cacheKey);
   }

   public List load(EntityInfo... entityInfos) {
      int size = entityInfos.length;
      ArrayList list = new ArrayList(size);
      for (EntityInfo e : entityInfos) {
         list.add(load(e));
      }
      return list;
   }

}
