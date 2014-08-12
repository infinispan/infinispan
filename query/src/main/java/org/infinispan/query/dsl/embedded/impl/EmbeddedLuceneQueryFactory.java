package org.infinispan.query.dsl.embedded.impl;

import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.infinispan.query.SearchManager;
import org.infinispan.query.dsl.QueryBuilder;
import org.infinispan.query.dsl.embedded.LuceneQuery;
import org.infinispan.query.dsl.impl.BaseQueryFactory;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public final class EmbeddedLuceneQueryFactory extends BaseQueryFactory<LuceneQuery> {

   private final SearchManager searchManager;

   private final EntityNamesResolver entityNamesResolver;

   public EmbeddedLuceneQueryFactory(SearchManager searchManager, EntityNamesResolver entityNamesResolver) {
      this.searchManager = searchManager;
      this.entityNamesResolver = entityNamesResolver;
   }

   @Override
   public QueryBuilder<LuceneQuery> from(Class type) {
      return new EmbeddedLuceneQueryBuilder(this, searchManager, entityNamesResolver, type.getCanonicalName());
   }

   @Override
   public QueryBuilder<LuceneQuery> from(String type) {
      return new EmbeddedLuceneQueryBuilder(this, searchManager, entityNamesResolver, type);
   }
}
