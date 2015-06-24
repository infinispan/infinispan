package org.infinispan.objectfilter.impl.hql;

import org.hibernate.hql.ast.spi.AstProcessingChain;
import org.hibernate.hql.ast.spi.AstProcessor;
import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.hibernate.hql.ast.spi.QueryRendererProcessor;
import org.hibernate.hql.ast.spi.QueryResolverProcessor;
import org.hibernate.hql.ast.spi.SingleEntityQueryBuilder;
import org.infinispan.objectfilter.impl.hql.predicate.FilterPredicateFactory;
import org.infinispan.objectfilter.impl.hql.predicate.SingleEntityHavingQueryBuilderImpl;
import org.infinispan.objectfilter.impl.syntax.BooleanExpr;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class FilterProcessingChain<TypeMetadata> implements AstProcessingChain<FilterParsingResult<TypeMetadata>> {

   private final List<AstProcessor> astProcessors;
   private final FilterRendererDelegate<TypeMetadata> rendererDelegate;

   private FilterProcessingChain(QueryResolverProcessor resolverProcessor, QueryRendererProcessor rendererProcessor, FilterRendererDelegate<TypeMetadata> rendererDelegate) {
      astProcessors = Arrays.asList(resolverProcessor, rendererProcessor);
      this.rendererDelegate = rendererDelegate;
   }

   @Override
   public Iterator<AstProcessor> iterator() {
      return astProcessors.iterator();
   }

   @Override
   public FilterParsingResult<TypeMetadata> getResult() {
      return rendererDelegate.getResult();
   }

   public static <TypeMetadata> FilterProcessingChain<TypeMetadata> build(EntityNamesResolver entityNamesResolver, ObjectPropertyHelper<TypeMetadata> propertyHelper, Map<String, Object> namedParameters) {
      QueryResolverProcessor resolverProcessor = new QueryResolverProcessor(new FilterQueryResolverDelegate(entityNamesResolver, propertyHelper));

      SingleEntityQueryBuilder<BooleanExpr> queryBuilder = SingleEntityQueryBuilder.getInstance(new FilterPredicateFactory(entityNamesResolver, propertyHelper), propertyHelper);
      SingleEntityHavingQueryBuilderImpl havingQueryBuilder = new SingleEntityHavingQueryBuilderImpl(entityNamesResolver, propertyHelper);

      FilterRendererDelegate<TypeMetadata> rendererDelegate = new FilterRendererDelegate<TypeMetadata>(entityNamesResolver, propertyHelper, queryBuilder, havingQueryBuilder, namedParameters);

      QueryRendererProcessor rendererProcessor = new QueryRendererProcessor(rendererDelegate);

      return new FilterProcessingChain<TypeMetadata>(resolverProcessor, rendererProcessor, rendererDelegate);
   }
}
