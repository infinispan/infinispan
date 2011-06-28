/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2011, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package org.infinispan.query.clustered;

import java.util.UUID;

import org.hibernate.search.query.engine.spi.HSQuery;
import org.infinispan.Cache;
import org.infinispan.query.clustered.commandworkers.CQCreateLazyQuery;
import org.infinispan.query.clustered.commandworkers.CQKillLazyIterator;
import org.infinispan.query.clustered.commandworkers.CQLazyFetcher;
import org.infinispan.query.clustered.commandworkers.ClusteredQueryCommandWorker;

/**
 * Types of ClusteredQueryCommandWorker. Each type defines a different behavior for a
 * ClusteredQueryCommand...
 * 
 * @author Israel Lacerra <israeldl@gmail.com>
 * @since 5.1
 */
public enum ClusteredQueryCommandType {

   CREATE_LAZY_ITERATOR() {
      @Override
      public ClusteredQueryCommandWorker getNewInstance() {
         return new CQCreateLazyQuery();
      }
   },
   DESTROY_LAZY_ITERATOR() {
      @Override
      public ClusteredQueryCommandWorker getNewInstance() {
         return new CQKillLazyIterator();
      }
   },
   GET_SOME_KEYS() {
      @Override
      public ClusteredQueryCommandWorker getNewInstance() {
         return new CQLazyFetcher();
      }
   };

   protected abstract ClusteredQueryCommandWorker getNewInstance();

   public ClusteredQueryCommandWorker getCommand(Cache cache, HSQuery query, UUID lazyQueryId,
            int docIndex) {
      ClusteredQueryCommandWorker command = null;
      command = getNewInstance();
      command.init(cache, query, lazyQueryId, docIndex);
      return command;
   }

}
