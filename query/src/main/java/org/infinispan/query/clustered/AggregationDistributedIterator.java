package org.infinispan.query.clustered;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.remoting.transport.Address;

public class AggregationDistributedIterator implements CloseableIterator<Object[]> {

   private final Iterator<Object[]> delegate;

   public AggregationDistributedIterator(Map<Address, NodeTopDocs> topDocsResponses, boolean displayGroupFirst, Sort sort) {
      delegate = merge(topDocsResponses, displayGroupFirst, sort).iterator();
   }

   private List<Object[]> merge(Map<Address, NodeTopDocs> topDocsResponses, boolean displayGroupFirst, Sort sort) {
      boolean sorted = false;
      boolean reverse = false;
      if (sort != null) {
         SortField[] sortFields = sort.getSort();
         if (sortFields.length == 1) {
            sorted = true;
            reverse = sortFields[0].getReverse();
         }
      }

      Map<Comparable<?>, Long> aggregations = (!sorted) ? new HashMap<>() : (reverse) ?
            new TreeMap<>(Collections.reverseOrder()) : new TreeMap<>();
      for (NodeTopDocs nodeTopDocs : topDocsResponses.values()) {
         Object[] projections = nodeTopDocs.projections;
         for (Object projection : projections) {
            Object[] items = (Object[]) projection;
            if (items.length != 2) {
               continue;
            }

            Comparable<?> group = (Comparable<?>) ((displayGroupFirst) ? items[0] : items[1]);
            Long value = (displayGroupFirst) ? (Long) items[1] : (Long) items[0];
            aggregations.merge(group, value, Long::sum);
         }
      }

      return aggregations.entrySet().stream()
            .map(entry -> {
               if (displayGroupFirst) {
                  return new Object[] {entry.getKey(), entry.getValue()};
               } else {
                  return new Object[] {entry.getValue(), entry.getKey()};
               }
            }).collect(Collectors.toList());
   }

   @Override
   public void close() {

   }

   @Override
   public boolean hasNext() {
      return delegate.hasNext();
   }

   @Override
   public Object[] next() {
      return delegate.next();
   }
}
