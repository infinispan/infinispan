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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.NoSuchElementException;
import java.util.UUID;

import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.PriorityQueue;
import org.infinispan.Cache;
import org.infinispan.query.impl.AbstractIterator;
import org.infinispan.util.ReflectionUtil;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * DistributedLazyIterator.
 * 
 * Iterates on a distributed query.
 * 
 * @author Israel Lacerra <israeldl@gmail.com>
 * @since 5.1
 */
public class DistributedLazyIterator extends AbstractIterator {

   private UUID queryId;

   private int fetchSize = 1;

   private int currentIndex = -1;

   // this array keeps all values (ordered) fetched by this iterator...
   private final ArrayList<Object> orderedValues = new ArrayList<Object>();

   private Cache cache;

   private final Sort sort;

   private HashMap<UUID, ClusteredTopDocs> topDocsResponses;

   private PriorityQueue<FieldDoc> hq;

   private final int resultSize;

   private static final Log log = LogFactory.getLog(DistributedLazyIterator.class);

   public DistributedLazyIterator(Sort sort, int fetchSize, int resultSize, UUID id,
            HashMap<UUID, ClusteredTopDocs> topDocsResponses, Cache cache) {
      this.sort = sort;
      this.fetchSize = fetchSize;
      this.resultSize = resultSize;
      queryId = id;
      this.cache = cache;
      setTopDocs(topDocsResponses);
   }

   private void setTopDocs(HashMap<UUID, ClusteredTopDocs> topDocsResponses) {
      this.topDocsResponses = topDocsResponses;

      if (sort != null) {
         // reversing sort fields to FieldDocSortedHitQueue work properly
         for (SortField sf : sort.getSort()) {
            boolean reverse = (Boolean) ReflectionUtil.getValue(sf, "reverse");
            ReflectionUtil.setValue(sf, "reverse", !reverse);
         }
         hq = ISPNPriorityQueueFactory.getFieldDocSortedHitQueue(topDocsResponses.size(), sort
                  .getSort());

      } else
         hq = ISPNPriorityQueueFactory.getHitQueue(topDocsResponses.size());

      // taking the first value of each queue
      for (ClusteredTopDocs ctp : topDocsResponses.values()) {
         if (ctp.hasNext())
            hq.add(ctp.getNext());
      }

   }

   @Override
   public void close() {
      ClusteredQueryCommand killQuery = ClusteredQueryCommand.destroyLazyQuery(cache, queryId);

      ClusteredQueryInvoker invoker = new ClusteredQueryInvoker(cache);
      try {
         invoker.broadcast(killQuery);
      } catch (Exception e) {
         log.error("Could not close the distributed iterator", e);
      }
   }

   @Override
   public void jumpToResult(int index) throws IndexOutOfBoundsException {
      currentIndex = index;
   }

   @Override
   public void add(Object arg0) {
      throw new UnsupportedOperationException(
               "Not supported as you are trying to change something in the cache.  Please use searchableCache.put()");
   }

   @Override
   public Object next() {
      if (!hasNext())
         throw new NoSuchElementException("Out of boundaries");
      currentIndex++;
      return current();
   }

   @Override
   public int nextIndex() {
      if (!hasNext())
         throw new NoSuchElementException("Out of boundaries");
      return currentIndex + 1;
   }

   @Override
   public Object previous() {
      currentIndex--;
      return current();
   }

   private Object current() {
      // if already fecthed
      if (orderedValues.size() > currentIndex) {
         return orderedValues.get(currentIndex);
      }

      // fetch and return the value
      loadTo(currentIndex);
      return orderedValues.get(currentIndex);
   }

   private void loadTo(int index) {
      int fetched = 0;

      while (orderedValues.size() <= index || fetched < fetchSize) {
         // getting the next scoreDoc. If null, then there is no more results
         ClusteredFieldDoc scoreDoc = (ClusteredFieldDoc) hq.pop();
         if (scoreDoc == null) {
            return;
         }

         // "recharging" the queue
         ClusteredTopDocs topDoc = topDocsResponses.get(scoreDoc.getNodeUuid());
         ClusteredFieldDoc score = topDoc.getNext();
         if (score != null) {
            hq.add(score);
         }

         // fetching the value
         ClusteredQueryInvoker invoker = new ClusteredQueryInvoker(cache);
         try {
            Object value = invoker.getValue(scoreDoc.index, topDoc.getNodeAddress(), queryId);
            orderedValues.add(value);
         } catch (Exception e) {
            log.error("Error while trying to remoting fetch next value: " + e.getMessage());
         }

         fetched++;
      }
   }

   @Override
   public int previousIndex() {
      return currentIndex - 1;
   }

   @Override
   public void remove() {
      throw new UnsupportedOperationException(
               "Not supported as you are trying to change something in the cache.  Please use searchableCache.put()");
   }

   @Override
   public void set(Object arg0) {
      throw new UnsupportedOperationException(
               "Not supported as you are trying to change something in the cache.  Please use searchableCache.put()");
   }

   public void setQueryId(UUID queryId) {
      this.queryId = queryId;
   }

   @Override
   public boolean hasNext() {
      if (currentIndex + 1 >= resultSize) {
         return false;
      }
      return true;
   }

}