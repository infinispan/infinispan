package org.infinispan.objectfilter.impl.hql;

import org.hibernate.hql.QueryParser;
import org.hibernate.hql.ast.spi.AstProcessingChain;
import org.hibernate.hql.ast.spi.AstProcessor;
import org.hibernate.hql.ast.spi.QueryRendererProcessor;
import org.hibernate.hql.ast.spi.QueryResolverProcessor;
import org.hibernate.hql.ast.spi.SingleEntityQueryBuilder;
import org.infinispan.objectfilter.impl.hql.predicate.FilterPredicateFactory;
import org.infinispan.objectfilter.impl.hql.predicate.SingleEntityHavingQueryBuilderImpl;
import org.infinispan.objectfilter.impl.syntax.BooleanExpr;
import org.infinispan.objectfilter.impl.syntax.ConstantValueExpr;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
public final class JPQLParser<TypeMetadata> {

   private static final QueryParser queryParser = new QueryParser();

   public JPQLParser() {
   }

   public FilterParsingResult<TypeMetadata> parse(String jpaQuery, ObjectPropertyHelper<TypeMetadata> propertyHelper) {

      QueryResolverProcessor resolverProcessor = new QueryResolverProcessor(new FilterQueryResolverDelegate(propertyHelper));

      FilterPredicateFactory predicateFactory = new FilterPredicateFactory(propertyHelper);

      SingleEntityQueryBuilder<BooleanExpr> queryBuilder = SingleEntityQueryBuilder.getInstance(predicateFactory, propertyHelper);

      SingleEntityHavingQueryBuilderImpl havingQueryBuilder = new SingleEntityHavingQueryBuilderImpl(propertyHelper.getEntityNamesResolver(), propertyHelper);

      FilterRendererDelegate<TypeMetadata> rendererDelegate = new FilterRendererDelegate<>(jpaQuery, propertyHelper, queryBuilder, havingQueryBuilder, makeParamPlaceholderMap());

      QueryRendererProcessor rendererProcessor = new QueryRendererProcessor(rendererDelegate);

      AstProcessingChain<FilterParsingResult<TypeMetadata>> chain = new AstProcessingChain<FilterParsingResult<TypeMetadata>>() {

         @Override
         public Iterator<AstProcessor> iterator() {
            return Arrays.asList(resolverProcessor, rendererProcessor).iterator();
         }

         @Override
         public FilterParsingResult<TypeMetadata> getResult() {
            return rendererDelegate.getResult();
         }
      };

      return queryParser.parseQuery(jpaQuery, chain);
   }

   /**
    * Create a 'magic' map that creates the requested elements out of the fly. This circumvents a design limitation in
    * SingleEntityQueryRendererDelegate.
    */
   private static Map<String, Object> makeParamPlaceholderMap() {
      return new HashMap<String, Object>(5) {
         @Override
         public Object get(Object key) {
            String keyAsString = (String) key;
            Object v = super.get(keyAsString);
            if (v == null) {
               v = new ConstantValueExpr.ParamPlaceholder(keyAsString);
               put(keyAsString, v);
            }
            return v;
         }
      };
   }
}
