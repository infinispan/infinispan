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

import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import org.apache.lucene.search.Sort;
import org.infinispan.Cache;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * DistributedLazyIterator.
 * 
 * Lazily iterates on a distributed query
 * 
 * @author Israel Lacerra <israeldl@gmail.com>
 * @since 5.1
 */
public class DistributedLazyIterator extends DistributedIterator {

   private UUID queryId;

   private final ExecutorService asyncExecutor;

   private static final Log log = LogFactory.getLog(DistributedLazyIterator.class);

   public DistributedLazyIterator(Sort sort, int fetchSize, int resultSize, UUID id,
            HashMap<UUID, ClusteredTopDocs> topDocsResponses, ExecutorService asyncExecutor, Cache cache) {
      super(sort, fetchSize, resultSize, topDocsResponses, cache);
      this.queryId = id;
      this.asyncExecutor = asyncExecutor;
   }

   @Override
   public void close() {
      ClusteredQueryCommand killQuery = ClusteredQueryCommand.destroyLazyQuery(cache, queryId);

      ClusteredQueryInvoker invoker = new ClusteredQueryInvoker(cache, asyncExecutor);
      try {
         invoker.broadcast(killQuery);
      } catch (Exception e) {
         log.error("Could not close the distributed iterator", e);
      }
   }

   @Override
   protected Object fetchValue(ClusteredDoc scoreDoc, ClusteredTopDocs topDoc) {
      ClusteredQueryInvoker invoker = new ClusteredQueryInvoker(cache, asyncExecutor);
      Object value = null;
      try {
         value = invoker.getValue(scoreDoc.getIndex(), topDoc.getNodeAddress(), queryId);
      } catch (Exception e) {
         log.error("Error while trying to remoting fetch next value: " + e.getMessage());
      }
      return value;
   }

}