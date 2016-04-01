package org.infinispan.query.dsl.embedded.impl.jpalucene;

import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.hibernate.search.bridge.FieldBridge;
import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.objectfilter.impl.hql.FilterParsingResult;

import java.util.Map;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
public final class JPALuceneTransformer {

   public static <TypeMetadata> LuceneQueryParsingResult<TypeMetadata> transform(FilterParsingResult<TypeMetadata> fpr,
                                                                                 SearchIntegrator searchFactory,
                                                                                 EntityNamesResolver entityNamesResolver,
                                                                                 FieldBridgeProvider fieldBridgeProvider,
                                                                                 Map<String, Object> namedParameters) {
      return new LuceneQueryMaker(searchFactory.buildQueryBuilder(), entityNamesResolver, fieldBridgeProvider, namedParameters).transform(fpr);
   }

   public interface FieldBridgeProvider {

      /**
       * Returns the field bridge to be applied when executing queries on the given property of the given entity type.
       *
       * @param typeName     the entity type hosting the given property; may either identify an actual Java type or a virtual type
       *                     managed by the given implementation; never {@code null}
       * @param propertyPath an array of strings denoting the property path; never {@code null}
       * @return the field bridge to be used for querying the given property; may be {@code null}
       */
      FieldBridge getFieldBridge(String typeName, String[] propertyPath);
   }
}
