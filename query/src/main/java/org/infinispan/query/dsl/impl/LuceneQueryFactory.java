package org.infinispan.query.dsl.impl;

import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.infinispan.query.SearchManager;
import org.infinispan.query.dsl.FilterConditionBeginContext;
import org.infinispan.query.dsl.FilterConditionEndContext;
import org.infinispan.query.dsl.QueryBuilder;
import org.infinispan.query.dsl.QueryFactory;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public class LuceneQueryFactory implements QueryFactory {

   private final SearchManager searchManager;

   private final EntityNamesResolver entityNamesResolver;

   public LuceneQueryFactory(SearchManager searchManager, EntityNamesResolver entityNamesResolver) {
      this.searchManager = searchManager;
      this.entityNamesResolver = entityNamesResolver;
   }

   @Override
   public QueryBuilder from(Class type) {
      return new LuceneQueryBuilder(searchManager, entityNamesResolver, type);
   }

   @Override
   public FilterConditionEndContext having(String attributePath) {
      return new AttributeCondition().having(attributePath);
   }

   @Override
   public FilterConditionBeginContext not() {
      return new AttributeCondition().not();
   }
}
