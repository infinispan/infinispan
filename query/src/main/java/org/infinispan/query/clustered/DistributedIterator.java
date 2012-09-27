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

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.PriorityQueue;
import org.infinispan.AdvancedCache;
import org.infinispan.query.QueryIterator;
import org.infinispan.util.ReflectionUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.NoSuchElementException;
import java.util.UUID;

/**
 * DistributedIterator.
 * 
 * Iterates on a distributed query.
 * 
 * @author Israel Lacerra <israeldl@gmail.com>
 * @since 5.1
 */
public class DistributedIterator implements QueryIterator {

   protected final AdvancedCache<?, ?> cache;

   private int currentIndex = -1;

   // this array keeps all values (ordered) fetched by this iterator...
   private final ArrayList<Object> orderedValues = new ArrayList<Object>();

   private final Sort sort;

   private final int fetchSize;

   private HashMap<UUID, ClusteredTopDocs> topDocsResponses;

   /**
    * Here we will have the top ScoreDoc of each node.
    * So, this queue will always have n scoreDocs (where n is the number of nodes)
    * This queue is actually doing the "merge" work
    */
   private PriorityQueue<ScoreDoc> hq;

   private final int resultSize;

   private final int maxResults;

   private final int firstResult;

   public DistributedIterator(Sort sort, int fetchSize, int resultSize, int maxResults, int firstResult,
         HashMap<UUID, ClusteredTopDocs> topDocsResponses, AdvancedCache<?, ?> cache) {
      this.sort = sort;
      this.fetchSize = fetchSize;
      this.resultSize = resultSize;
      this.maxResults = maxResults;
      this.firstResult = firstResult;
      this.cache = cache;
      setTopDocs(topDocsResponses);
      goToFirstResult();
   }

   /**
    * As we don't know where (in what node) is the first result,
    * we have to compare the results until we achieve the first result. 
    */
   private void goToFirstResult() {
      for (int i = 0; i < firstResult; i++) {
         // getting the next scoreDoc. If null, then there is no more results
         ClusteredDoc scoreDoc = (ClusteredDoc) hq.pop();
         if (scoreDoc == null) {
            return;
         }
         rechargeQueue(scoreDoc);
      }
   }

   private void setTopDocs(HashMap<UUID, ClusteredTopDocs> topDocsResponses) {
      this.topDocsResponses = topDocsResponses;

      if (sort != null) {
         // reversing sort fields to FieldDocSortedHitQueue work properly
         for (SortField sf : sort.getSort()) {
            boolean reverse = (Boolean) ReflectionUtil.getValue(sf, "reverse");
            ReflectionUtil.setValue(sf, "reverse", !reverse);
         }
         hq = ISPNPriorityQueueFactory.getFieldDocSortedHitQueue(
               topDocsResponses.size(), sort.getSort());

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
      // Nothing to do...
   }

   @Override
   public void jumpToIndex(int index) throws IndexOutOfBoundsException {
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
         ClusteredDoc scoreDoc = (ClusteredDoc) hq.pop();
         if (scoreDoc == null) {
            return;
         }

         ClusteredTopDocs topDoc = rechargeQueue(scoreDoc);

         // fetching the value
         Object value = fetchValue(scoreDoc, topDoc);

         orderedValues.add(value);

         fetched++;
      }
   }

   /**
    * As the priority queue have one result for each node, when we fetch a result
    * we have to recharge the queue (getting the next score doc from the correct
    * node)
    * @param scoreDoc
    * @return
    */
   private ClusteredTopDocs rechargeQueue(ClusteredDoc scoreDoc) {
      // "recharging" the queue
      // the queue has a top element of each node. As we removed a element, we have to get the next element from this node and put on the queue.
      ClusteredTopDocs topDoc = topDocsResponses.get(scoreDoc.getNodeUuid());
      ScoreDoc score = topDoc.getNext();
      // if score == null -> this node does not have more results...
      if (score != null) {
         hq.add(score);
      }
      return topDoc;
   }

   protected Object fetchValue(ClusteredDoc scoreDoc, ClusteredTopDocs topDoc) {
      ISPNEagerTopDocs eagerTopDocs = (ISPNEagerTopDocs) topDoc.getTopDocs();
      return cache.get(eagerTopDocs.keys[scoreDoc.getIndex()]);
   }

   @Override
   public int previousIndex() {
      return currentIndex - 1;
   }

   @Override
   public void beforeFirst() {
      currentIndex = 0;
   }

   @Override
   public void afterLast() {
      currentIndex = resultSize;
   }

   @Override
   public boolean hasPrevious() {
      return currentIndex > 0;
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

   @Override
   public boolean hasNext() {
      int nextIndex = currentIndex + 1;
      if (firstResult + nextIndex >= resultSize || nextIndex >= maxResults) {
         return false;
      }
      return true;
   }

}
