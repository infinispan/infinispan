package org.infinispan.query.remote.search;

import com.google.protobuf.Descriptors;
import org.apache.lucene.search.Query;
import org.hibernate.hql.ast.spi.AstProcessingChain;
import org.hibernate.hql.ast.spi.AstProcessor;
import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.hibernate.hql.ast.spi.QueryRendererProcessor;
import org.hibernate.hql.ast.spi.QueryResolverProcessor;
import org.hibernate.hql.ast.spi.SingleEntityQueryBuilder;
import org.hibernate.hql.lucene.LuceneQueryParsingResult;
import org.hibernate.hql.lucene.internal.LuceneQueryRendererDelegate;
import org.hibernate.hql.lucene.internal.builder.LucenePropertyHelper;
import org.hibernate.search.spi.SearchFactoryIntegrator;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

/**
 * AST processing chain for creating Lucene queries from HQL queries.
 *
 * @author Gunnar Morling
 * @author anistor@redhat.com
 */
public class IspnLuceneProcessingChain implements AstProcessingChain<IspnLuceneQueryParsingResult> {

   private static final Integer TRUE_INT = 1;
   private static final Integer FALSE_INT = 0;

   private final QueryResolverProcessor resolverProcessor;
   private final QueryRendererProcessor rendererProcessor;
   private final LuceneQueryRendererDelegate rendererDelegate;
   private final IspnLuceneQueryResolverDelegate queryResolverDelegate;

   public IspnLuceneProcessingChain(SearchFactoryIntegrator searchFactory, EntityNamesResolver entityNames, Map<String, Object> namedParameters) {
      queryResolverDelegate = new IspnLuceneQueryResolverDelegate();
      resolverProcessor = new QueryResolverProcessor(queryResolverDelegate);

      LucenePropertyHelper propertyHelper = new LucenePropertyHelper(searchFactory) {
         @Override
         public Object convertToPropertyType(String value, Class<?> entityType, String... propertyPath) {
            Descriptors.FieldDescriptor field = queryResolverDelegate.getTargetType().findFieldByName(propertyPath[propertyPath.length - 1]);
            if (field != null) {
               switch (field.getJavaType()) {
                  case INT:
                     return Integer.parseInt(value);
                  case LONG:
                     return Long.parseLong(value);
                  case FLOAT:
                     return Float.parseFloat(value);
                  case DOUBLE:
                     return Double.parseDouble(value);
                  case BOOLEAN:
                     return Boolean.valueOf(value) ? TRUE_INT : FALSE_INT;
                  case ENUM:
                     return field.getEnumType().findValueByName(value).getNumber();
               }
            }
            return value;
         }
      };

      SingleEntityQueryBuilder<Query> queryBuilder = SingleEntityQueryBuilder.getInstance(new IspnLucenePredicateFactory(searchFactory.buildQueryBuilder(), propertyHelper), propertyHelper);

      rendererDelegate = new LuceneQueryRendererDelegate(entityNames, queryBuilder, namedParameters);
      rendererProcessor = new QueryRendererProcessor(rendererDelegate);
   }

   @Override
   public Iterator<AstProcessor> iterator() {
      return Arrays.asList(resolverProcessor, rendererProcessor).iterator();
   }

   @Override
   public IspnLuceneQueryParsingResult getResult() {
      LuceneQueryParsingResult result = rendererDelegate.getResult();
      return new IspnLuceneQueryParsingResult(result.getQuery(), queryResolverDelegate.getTargetType(), result.getTargetEntity(), result.getProjections());
   }
}
