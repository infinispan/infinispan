package org.infinispan.query.remote.search;

import org.apache.lucene.search.Query;
import org.hibernate.hql.ast.spi.predicate.*;
import org.hibernate.hql.internal.util.Strings;
import org.hibernate.hql.lucene.internal.builder.LucenePropertyHelper;
import org.hibernate.hql.lucene.internal.builder.predicate.LuceneConjunctionPredicate;
import org.hibernate.hql.lucene.internal.builder.predicate.LuceneDisjunctionPredicate;
import org.hibernate.hql.lucene.internal.builder.predicate.LuceneNegationPredicate;
import org.hibernate.hql.lucene.internal.builder.predicate.LuceneRootPredicate;
import org.hibernate.search.engine.metadata.impl.TypeMetadata;
import org.hibernate.search.query.dsl.QueryBuilder;
import org.hibernate.search.query.dsl.QueryContextBuilder;
import org.infinispan.query.remote.QueryFacadeImpl;

import java.util.List;

/**
 * Factory creating predicate instances based on Lucene.
 *
 * @author Gunnar Morling
 */
class IspnLucenePredicateFactory implements PredicateFactory<Query> {

   private final QueryContextBuilder queryContextBuilder;
   private final LucenePropertyHelper propertyHelper;
   private QueryBuilder queryBuilder;

   public IspnLucenePredicateFactory(QueryContextBuilder queryContextBuilder, LucenePropertyHelper propertyHelper) {
      this.queryContextBuilder = queryContextBuilder;
      this.propertyHelper = propertyHelper;
   }

   @Override
   public RootPredicate<Query> getRootPredicate(Class<?> entityType) {
      queryBuilder = queryContextBuilder.forEntity(entityType).get();
      return new LuceneRootPredicate(queryBuilder);
   }

   @Override
   public ComparisonPredicate<Query> getComparisonPredicate(Class<?> entityType, ComparisonPredicate.Type comparisonType, List<String> propertyPath, Object value) {
      return new IspnLuceneComparisonPredicate(queryBuilder, Strings.join(propertyPath, "."), comparisonType, value);
   }

   @Override
   public InPredicate<Query> getInPredicate(Class<?> entityType, List<String> propertyPath, List<Object> values) {
      return new IspnLuceneInPredicate(queryBuilder, Strings.join(propertyPath, "."), values);
   }

   @Override
   public RangePredicate<Query> getRangePredicate(Class<?> entityType, List<String> propertyPath, Object lowerValue, Object upperValue) {
      return new IspnLuceneRangePredicate(queryBuilder, Strings.join(propertyPath, "."), lowerValue, upperValue);
   }

   @Override
   public NegationPredicate<Query> getNegationPredicate() {
      return new LuceneNegationPredicate(queryBuilder);
   }

   @Override
   public DisjunctionPredicate<Query> getDisjunctionPredicate() {
      return new LuceneDisjunctionPredicate(queryBuilder);
   }

   @Override
   public ConjunctionPredicate<Query> getConjunctionPredicate() {
      return new LuceneConjunctionPredicate(queryBuilder);
   }

   @Override
   public LikePredicate<Query> getLikePredicate(Class<?> entityType, List<String> propertyPath, String patternValue, Character escapeCharacter) {
      return new IspnLuceneLikePredicate(queryBuilder, Strings.join(propertyPath, "."), patternValue);
   }

   @Override
   public IsNullPredicate<Query> getIsNullPredicate(Class<?> entityType, List<String> propertyPath) {
      TypeMetadata typeMetadata = propertyHelper.getLeafTypeMetadata(entityType, propertyPath.toArray(new String[propertyPath.size()]));

//      String nullToken;
//      if (propertyHelper.isEmbedded(entityType, propertyPath)) {
//         nullToken = ((EmbeddedTypeMetadata) typeMetadata).getEmbeddedNullToken();
//      } else {
//         PropertyMetadata propertyMetadata = typeMetadata.getPropertyMetadataForProperty(propertyPath.get(propertyPath.size() - 1));
//         nullToken = propertyMetadata.getFieldMetadata().iterator().next().indexNullAs();
//      }
//
//      return new IspnLuceneIsNullPredicate(queryBuilder, Strings.join(propertyPath, "."), nullToken);

      //todo [anistor] how can a user define the null token?
      return new IspnLuceneIsNullPredicate(queryBuilder, Strings.join(propertyPath, "."), QueryFacadeImpl.NULL_TOKEN);
   }

   public static boolean isNumericValue(Object value) {
      if (value != null) {
         Class numericClass = value.getClass();
         return numericClass.isAssignableFrom(Double.class) ||
               numericClass.isAssignableFrom(Long.class) ||
               numericClass.isAssignableFrom(Integer.class) ||
               numericClass.isAssignableFrom(Float.class);
      }
      return false;
   }
}
