package org.infinispan.query.test;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.CachingWrapperQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.TermQuery;
import org.hibernate.search.annotations.Factory;

/**
 * Filter for using in full text search. Filters persons by the blurb.
 *
 * @author Anna Manukyan
 */
public class PersonBlurbFilterFactory {
   private String blurbText;

   public void setBlurbText(String blurbText) {
      this.blurbText = blurbText;
   }

   @Factory
   public Filter getFilter() {
      BooleanQuery query = new BooleanQuery.Builder()
              .add(new TermQuery(new Term("blurb", blurbText)), BooleanClause.Occur.FILTER)
              .build();
      return new QueryWrapperFilter(new CachingWrapperQuery(query));
   }
}
