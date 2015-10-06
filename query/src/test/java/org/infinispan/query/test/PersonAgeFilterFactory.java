package org.infinispan.query.test;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.QueryWrapperFilter;
import org.hibernate.search.annotations.Factory;

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

   public void setAge(Integer age) {
      this.age = age;
   }

   @Factory
   public Filter getFilter() {
      NumericRangeQuery<Integer> query = NumericRangeQuery.newIntRange("age", this.age, age, true, true);
      BooleanQuery filterQuery = new BooleanQuery.Builder().add(query, BooleanClause.Occur.FILTER).build();
      return new QueryWrapperFilter(filterQuery);
   }
}
