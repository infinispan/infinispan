package org.infinispan.objectfilter.impl.hql;

import org.hibernate.hql.ast.spi.AstProcessingChain;
import org.hibernate.hql.ast.spi.AstProcessor;
import org.hibernate.hql.ast.spi.QueryRendererProcessor;
import org.hibernate.hql.ast.spi.QueryResolverProcessor;
import org.hibernate.hql.ast.spi.SingleEntityQueryBuilder;
import org.infinispan.objectfilter.impl.hql.predicate.FilterPredicateFactory;
import org.infinispan.objectfilter.impl.syntax.BooleanExpr;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class FilterProcessingChain<TypeMetadata> implements AstProcessingChain<FilterParsingResult> {

   private final QueryResolverProcessor resolverProcessor;
   private final QueryRendererProcessor rendererProcessor;
   private final FilterRendererDelegate rendererDelegate;

   private FilterProcessingChain(QueryResolverProcessor resolverProcessor, QueryRendererProcessor rendererProcessor, FilterRendererDelegate rendererDelegate) {
      this.resolverProcessor = resolverProcessor;
      this.rendererProcessor = rendererProcessor;
      this.rendererDelegate = rendererDelegate;
   }

   @Override
   public Iterator<AstProcessor> iterator() {
      return Arrays.asList(resolverProcessor, rendererProcessor).iterator();
   }

   @Override
   public FilterParsingResult getResult() {
      return rendererDelegate.getResult();
   }

   public static <TypeMetadata> FilterProcessingChain<TypeMetadata> build(ObjectPropertyHelper<TypeMetadata> propertyHelper, Map<String, Object> namedParameters) {
      QueryResolverProcessor resolverProcessor = new QueryResolverProcessor(new FilterQueryResolverDelegate(propertyHelper));

      SingleEntityQueryBuilder<BooleanExpr> queryBuilder = SingleEntityQueryBuilder.getInstance(new FilterPredicateFactory(propertyHelper.getEntityNamesResolver()), propertyHelper);

      FilterRendererDelegate rendererDelegate = new FilterRendererDelegate(propertyHelper, queryBuilder, namedParameters);

      QueryRendererProcessor rendererProcessor = new QueryRendererProcessor(rendererDelegate);

      return new FilterProcessingChain<TypeMetadata>(resolverProcessor, rendererProcessor, rendererDelegate);
   }
}
