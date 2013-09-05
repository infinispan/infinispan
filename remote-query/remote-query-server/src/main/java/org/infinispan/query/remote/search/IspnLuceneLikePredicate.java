package org.infinispan.query.remote.search;

import org.apache.lucene.search.Query;
import org.hibernate.hql.ast.spi.predicate.LikePredicate;
import org.hibernate.search.query.dsl.QueryBuilder;

import java.util.regex.Pattern;

/**
 * Lucene-based {@code LIKE} predicate.
 *
 * @author Gunnar Morling
 */
class IspnLuceneLikePredicate extends LikePredicate<Query> {

   private static final String LUCENE_SINGLE_CHARACTER_WILDCARD = "?";
   private static final String LUCENE_MULTIPLE_CHARACTERS_WILDCARD = "*";

   private static final Pattern MULTIPLE_CHARACTERS_WILDCARD_PATTERN = Pattern.compile("%");
   private static final Pattern SINGLE_CHARACTER_WILDCARD_PATTERN = Pattern.compile("_");

   private final QueryBuilder builder;

   public IspnLuceneLikePredicate(QueryBuilder builder, String propertyName, String patternValue) {
      super(propertyName, patternValue, null);
      this.builder = builder;
   }

   @Override
   public Query getQuery() {
      String patternValue = MULTIPLE_CHARACTERS_WILDCARD_PATTERN.matcher(this.patternValue).replaceAll(LUCENE_MULTIPLE_CHARACTERS_WILDCARD);
      patternValue = SINGLE_CHARACTER_WILDCARD_PATTERN.matcher(patternValue).replaceAll(LUCENE_SINGLE_CHARACTER_WILDCARD);

      return builder.keyword().wildcard().onField(propertyName).ignoreFieldBridge().matching(patternValue).createQuery();
   }
}
