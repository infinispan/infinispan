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
 * A filter factory producing a filter for Person. Filters by age.
 *
 * The main purpose of this filter is to work together with other filters and test
 * applying more than one filter.
 *
 * @author Martin Gencur
 */
public class PersonAgeFilterFactory {
   private Integer age;

   @Key
   public FilterKey getKey() {
      StandardFilterKey key = new StandardFilterKey();
      key.addParameter(age);
      return key;
   }

   public void setAge(Integer age) {
      this.age = age;
   }

   @Factory
   public Filter getFilter() {
      Query query = new TermQuery(new Term("age", age.toString()));
      return new CachingWrapperFilter(new QueryWrapperFilter(query));
   }
}
