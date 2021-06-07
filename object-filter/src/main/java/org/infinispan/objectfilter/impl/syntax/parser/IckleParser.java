package org.infinispan.objectfilter.impl.syntax.parser;

import org.infinispan.objectfilter.ParsingException;
import org.infinispan.objectfilter.impl.ql.QueryParser;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
public final class IckleParser {

   private static final QueryParser queryParser = new QueryParser();

   private IckleParser() {
   }

   public static <TypeMetadata> IckleParsingResult<TypeMetadata> parse(String queryString, ObjectPropertyHelper<TypeMetadata> propertyHelper) {
      QueryResolverDelegateImpl<TypeMetadata> resolverDelegate = new QueryResolverDelegateImpl<>(propertyHelper);
      QueryRendererDelegateImpl<TypeMetadata> rendererDelegate = new QueryRendererDelegateImpl<>(queryString, propertyHelper);
      queryParser.parseQuery(queryString, resolverDelegate, rendererDelegate);
      IckleParsingResult<TypeMetadata> result = rendererDelegate.getResult();
      if (result.getStatementType() == IckleParsingResult.StatementType.DELETE
            && (result.getProjections() != null || result.getSortFields() != null || result.getGroupBy() != null)) {
         throw new ParsingException("DELETE statements cannot have projections or use ORDER BY or GROUP BY");
      }
      return result;
   }
}
