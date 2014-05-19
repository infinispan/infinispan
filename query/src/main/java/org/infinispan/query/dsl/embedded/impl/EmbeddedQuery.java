package org.infinispan.query.dsl.embedded.impl;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.util.CloseableIterable;
import org.infinispan.objectfilter.Matcher;
import org.infinispan.query.dsl.Query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * @author anistor@redhat,com
 * @since 7.0
 */
public final class EmbeddedQuery implements Query {

   private final AdvancedCache<?, ?> cache;

   private final String jpaQuery;

   private final Class<? extends Matcher> matcherImplClass;

   private List results;

   private String[] projection;

   public EmbeddedQuery(AdvancedCache<?, ?> cache, String jpaQuery, Class<? extends Matcher> matcherImplClass) {
      this.cache = cache;
      this.jpaQuery = jpaQuery;
      this.matcherImplClass = matcherImplClass;
   }

   public String[] getProjection() {
      return projection;
   }

   @Override
   public <T> List<T> list() {
      if (results == null) {
         FilterAndConverter filter = new FilterAndConverter(jpaQuery, matcherImplClass);
         CloseableIterable<Map.Entry<?, T>> iterable = cache.filterEntries(filter).converter(filter);

         results = new ArrayList<T>();
         try {
            for (Map.Entry<?, T> entry : iterable) {
               results.add(entry.getValue());
            }
         } finally {
            try {
               iterable.close();
            } catch (Exception e) {
               // exception ignored
            }
         }

         projection = filter.getProjection();
      }

      return results;
   }

   @Override
   public int getResultSize() {
      return list().size();
   }
}
