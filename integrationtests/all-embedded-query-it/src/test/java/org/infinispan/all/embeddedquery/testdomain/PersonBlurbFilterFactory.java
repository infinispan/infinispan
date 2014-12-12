package org.infinispan.all.embeddedquery.testdomain;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.CachingWrapperFilter;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.TermQuery;
import org.hibernate.search.annotations.Factory;
import org.hibernate.search.annotations.Key;
import org.hibernate.search.filter.FilterKey;
import org.hibernate.search.filter.StandardFilterKey;

/**
 * Filter for using in full text search. Filters persons by the blurb.
 *
 * @author Anna Manukyan
 */
public class PersonBlurbFilterFactory {
   private String blurbText;

   @Key
   public FilterKey getKey() {
      StandardFilterKey key = new StandardFilterKey();
      key.addParameter(blurbText);
      return key;
   }

   public void setBlurbText(String blurbText) {
      this.blurbText = blurbText;
   }

   @Factory
   public Filter getFilter() {
      Query query = new TermQuery(new Term("blurb", blurbText));
      return new CachingWrapperFilter(new QueryWrapperFilter(query));
   }
}