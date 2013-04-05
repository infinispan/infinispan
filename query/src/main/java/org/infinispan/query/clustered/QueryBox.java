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

import org.hibernate.search.query.engine.spi.DocumentExtractor;
import org.infinispan.AdvancedCache;
import org.infinispan.query.backend.KeyTransformationHandler;
import org.infinispan.query.clustered.commandworkers.QueryExtractorUtil;
import org.infinispan.util.CollectionFactory;

import java.util.LinkedList;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

/**
 * Each node in the cluster has a QueryBox instance. The QueryBox keep the active lazy iterators
 * (actually it keeps the DocumentExtractor of the searches) on the cluster, so it can return values
 * for the queries in a "lazy" way.
 * 
 * When a DistributedLazyIterator is created, every nodes creates a DocumentExtractor and register
 * it in your own QueryBox. So, the LazyIterator can fetch the values in a lazy way.
 * 
 * EVICTION: Currently the QueryBox keeps the last BOX_LIMIT DocumentExtractor used... probably
 * there is a better way.
 * 
 * @author Israel Lacerra <israeldl@gmail.com>
 * @since 5.1
 */
public class QueryBox {

   // <query UUID, ISPNQuery>
   private final ConcurrentMap<UUID, DocumentExtractor> queries = CollectionFactory.makeConcurrentMap();

   // queries UUIDs ordered (for eviction)
   private final LinkedList<UUID> ageOrderedQueries = new LinkedList<UUID>();

   // For eviction. Probably there is a better way...
   private static final int BOX_LIMIT = 3000;

   // this id will be sent with the responses to rpcs
   private final UUID myId = UUID.randomUUID();

   // the local cache instance
   private AdvancedCache<?, ?> cache;

   private KeyTransformationHandler keyTransformationHandler;

   /**
    * Get the "docIndex" value on the correct DocumentExtractor
    * 
    * @param queryUuid
    *           The queryId, so we can get the correct DocumentExtractor
    * @param docIndex
    *           value index in the DocumentExtractor
    * @return
    */
   public Object getValue(UUID queryUuid, int docIndex) {
      touch(queryUuid);

      DocumentExtractor extractor = queries.get(queryUuid);

      if (extractor == null) {
         throw new IllegalStateException("Query not found!");
      }

      Object key = QueryExtractorUtil.extractKey(extractor, cache, keyTransformationHandler, docIndex);
      return cache.get(key);
   }

   private void touch(UUID id) {
      synchronized (ageOrderedQueries) {
         ageOrderedQueries.remove(id);
         ageOrderedQueries.addFirst(id);
      }
   }

   /**
    * Kill the query (DocumentExtractor)
    * 
    * @param id
    *           The id of the query
    */
   public void kill(UUID id) {
      DocumentExtractor extractor = queries.remove(id);
      ageOrderedQueries.remove(id);
      if (extractor != null)
         extractor.close();
   }

   /**
    * Register a query (DocumentExtractor), so we can lazily load the results.
    * 
    * @param id
    *           The id of the query
    * @param extractor
    *           The query
    */
   public synchronized void put(UUID id, DocumentExtractor extractor) {
      synchronized (ageOrderedQueries) {
         if (ageOrderedQueries.size() >= BOX_LIMIT) {
            ageOrderedQueries.removeLast();
         }
         ageOrderedQueries.add(id);
      }

      queries.put(id, extractor);
   }

   /**
    * Id of this QueryBox
    * 
    * @return
    */
   public UUID getMyId() {
      return myId;
   }

   public void setCache(AdvancedCache<?, ?> cache) {
      this.cache = cache;
      keyTransformationHandler = KeyTransformationHandler.getInstance(cache.getAdvancedCache());
   }

}
