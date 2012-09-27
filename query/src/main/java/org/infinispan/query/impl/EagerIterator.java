/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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

import java.util.List;

import net.jcip.annotations.NotThreadSafe;

import org.hibernate.search.query.engine.spi.EntityInfo;

/**
 * This is the implementation class for the interface QueryResultIterator which extends ListIterator. It is what is
 * returned when the {@link org.infinispan.query.CacheQuery#iterator()}.
 * <p/>
 * <p/>
 *
 * @author Navin Surtani
 * @author Marko Luksa
 */
@NotThreadSafe
public class EagerIterator extends AbstractIterator {

   private List<EntityInfo> entityInfos;

   public EagerIterator(List<EntityInfo> entityInfos, QueryResultLoader resultLoader, int fetchSize) {
      super(resultLoader, fetchSize);
      this.entityInfos = entityInfos;

      max = entityInfos.size() - 1;
   }

   @Override
   public void close() {
      // This method does not need to do anything for this type of iterator as when an instance of it is
      // created, the iterator() method in CacheQueryImpl closes everything that needs to be closed.
   }

   protected EntityInfo loadEntityInfo(int index) {
      return entityInfos.get(index);
   }

}
