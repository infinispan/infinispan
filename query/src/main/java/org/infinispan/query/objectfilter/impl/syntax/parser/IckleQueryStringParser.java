package org.infinispan.query.objectfilter.impl.syntax.parser;

import org.infinispan.query.grammar.IckleParser;
import org.infinispan.query.objectfilter.ParsingException;
import org.infinispan.query.objectfilter.impl.ql.QueryParser;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
public final class IckleQueryStringParser {

   private static final QueryParser queryParser = new QueryParser();

   private IckleQueryStringParser() {
   }

   public static <TypeMetadata> IckleParsingResult<TypeMetadata> parse(String queryString, ObjectPropertyHelper<TypeMetadata> propertyHelper) {
      IckleParser.StatementContext tree = queryParser.parseQuery(queryString);
      IckleParserVisitorResult visitor = new IckleParserVisitorResult(queryString, propertyHelper);
      visitor.visit(tree);

      IckleParsingResult result = visitor.getParsingResult();

      if (result.getStatementType() == IckleParsingResult.StatementType.DELETE
            && (result.getProjections() != null || result.getSortFields() != null || result.getGroupBy() != null)) {
         throw new ParsingException("DELETE statements cannot have projections or use ORDER BY or GROUP BY");
      }
      return result;
   }
}
