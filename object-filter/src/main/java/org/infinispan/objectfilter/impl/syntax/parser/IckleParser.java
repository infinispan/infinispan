package org.infinispan.objectfilter.impl.syntax.parser;

import org.infinispan.objectfilter.impl.ql.QueryParser;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
public final class IckleParser {

   private static final QueryParser queryParser = new QueryParser();

   private IckleParser() {
   }

   public static <TypeMetadata> FilterParsingResult<TypeMetadata> parse(String queryString, ObjectPropertyHelper<TypeMetadata> propertyHelper) {
      QueryResolverDelegateImpl resolverDelegate = new QueryResolverDelegateImpl<>(propertyHelper);
      QueryRendererDelegateImpl<TypeMetadata> rendererDelegate = new QueryRendererDelegateImpl<>(queryString, propertyHelper);
      queryParser.parseQuery(queryString, resolverDelegate, rendererDelegate);
      return rendererDelegate.getResult();
   }
}
