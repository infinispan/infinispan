package org.infinispan.query.dsl.embedded.impl;

import java.io.IOException;
import java.io.StringReader;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermRangeQuery;
import org.hibernate.search.analyzer.impl.LuceneAnalyzerReference;
import org.hibernate.search.bridge.FieldBridge;
import org.hibernate.search.bridge.builtin.NumericFieldBridge;
import org.hibernate.search.bridge.builtin.impl.NullEncodingTwoWayFieldBridge;
import org.hibernate.search.engine.integration.impl.ExtendedSearchIntegrator;
import org.hibernate.search.query.dsl.BooleanJunction;
import org.hibernate.search.query.dsl.FieldCustomization;
import org.hibernate.search.query.dsl.PhraseContext;
import org.hibernate.search.query.dsl.QueryBuilder;
import org.hibernate.search.query.dsl.QueryContextBuilder;
import org.hibernate.search.query.dsl.RangeMatchingContext;
import org.hibernate.search.query.dsl.RangeTerminationExcludable;
import org.hibernate.search.query.dsl.impl.FieldBridgeCustomization;
import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.objectfilter.impl.ql.PropertyPath;
import org.infinispan.objectfilter.impl.syntax.AggregationExpr;
import org.infinispan.objectfilter.impl.syntax.AndExpr;
import org.infinispan.objectfilter.impl.syntax.BetweenExpr;
import org.infinispan.objectfilter.impl.syntax.BooleanExpr;
import org.infinispan.objectfilter.impl.syntax.ComparisonExpr;
import org.infinispan.objectfilter.impl.syntax.ConstantBooleanExpr;
import org.infinispan.objectfilter.impl.syntax.ConstantValueExpr;
import org.infinispan.objectfilter.impl.syntax.FullTextBoostExpr;
import org.infinispan.objectfilter.impl.syntax.FullTextOccurExpr;
import org.infinispan.objectfilter.impl.syntax.FullTextRangeExpr;
import org.infinispan.objectfilter.impl.syntax.FullTextRegexpExpr;
import org.infinispan.objectfilter.impl.syntax.FullTextTermExpr;
import org.infinispan.objectfilter.impl.syntax.IsNullExpr;
import org.infinispan.objectfilter.impl.syntax.LikeExpr;
import org.infinispan.objectfilter.impl.syntax.NotExpr;
import org.infinispan.objectfilter.impl.syntax.OrExpr;
import org.infinispan.objectfilter.impl.syntax.PropertyValueExpr;
import org.infinispan.objectfilter.impl.syntax.Visitor;
import org.infinispan.objectfilter.impl.syntax.parser.FilterParsingResult;
import org.infinispan.query.logging.Log;
import org.jboss.logging.Logger;

/**
 * An *Expr {@link Visitor} that transforms a {@link FilterParsingResult} into a {@link LuceneQueryParsingResult}.
 * <p>
 * NOTE: This is not stateless, not threadsafe, so it can only be used for a single transformation at a time.
 *
 * @author anistor@redhat.com
 * @since 9.0
 */
public final class LuceneQueryMaker<TypeMetadata> implements Visitor<Query, Query> {

   private static final Log log = Logger.getMessageLogger(Log.class, LuceneQueryMaker.class.getName());

   private static final char LUCENE_SINGLE_CHARACTER_WILDCARD = '?';
   private static final char LUCENE_MULTIPLE_CHARACTERS_WILDCARD = '*';
   private static final char LUCENE_WILDCARD_ESCAPE_CHARACTER = '\\';

   private final QueryContextBuilder queryContextBuilder;
   private final FieldBridgeProvider<TypeMetadata> fieldBridgeProvider;
   private final SearchIntegrator searchFactory;

   private Map<String, Object> namedParameters;
   private QueryBuilder queryBuilder;
   private TypeMetadata entityType;
   private Analyzer entityAnalyzer;

   @FunctionalInterface
   public interface FieldBridgeProvider<TypeMetadata> {

      /**
       * Returns the field bridge to be applied when executing queries on the given property of the given entity type.
       *
       * @param typeMetadata the entity type hosting the given property; may either identify an actual Java type or a
       *                     virtual type managed by the given implementation; never {@code null}
       * @param propertyPath an array of strings denoting the property path; never {@code null}
       * @return the field bridge to be used for querying the given property; may be {@code null}
       */
      FieldBridge getFieldBridge(TypeMetadata typeMetadata, String[] propertyPath);
   }

   public LuceneQueryMaker(SearchIntegrator searchFactory, FieldBridgeProvider<TypeMetadata> fieldBridgeProvider) {
      if (searchFactory == null) {
         throw new IllegalArgumentException("searchFactory argument cannot be null");
      }
      this.fieldBridgeProvider = fieldBridgeProvider;
      this.queryContextBuilder = searchFactory.buildQueryBuilder();
      this.searchFactory = searchFactory;
   }

   public LuceneQueryParsingResult<TypeMetadata> transform(FilterParsingResult<TypeMetadata> parsingResult, Map<String, Object> namedParameters, Class<?> targetedType) {
      this.namedParameters = namedParameters;
      queryBuilder = queryContextBuilder.forEntity(targetedType).get();
      entityType = parsingResult.getTargetEntityMetadata();
      entityAnalyzer = ((ExtendedSearchIntegrator) searchFactory).getAnalyzerReference(targetedType).unwrap(LuceneAnalyzerReference.class).getAnalyzer();
      Query query = makeQuery(parsingResult.getWhereClause());
      Sort sort = makeSort(parsingResult.getSortFields());
      return new LuceneQueryParsingResult<>(query, parsingResult.getTargetEntityName(), parsingResult.getTargetEntityMetadata(), parsingResult.getProjections(), sort);
   }

   private Query makeQuery(BooleanExpr expr) {
      return expr == null ? queryBuilder.all().createQuery() : expr.acceptVisitor(this);
   }

   private Sort makeSort(org.infinispan.objectfilter.SortField[] sortFields) {
      if (sortFields == null || sortFields.length == 0) {
         return null;
      }

      SortField[] fields = new SortField[sortFields.length];
      for (int i = 0; i < fields.length; i++) {
         org.infinispan.objectfilter.SortField sf = sortFields[i];
         SortField.Type sortType = SortField.Type.STRING;
         FieldBridge fieldBridge = fieldBridgeProvider.getFieldBridge(entityType, sf.getPath().asArrayPath());
         if (fieldBridge instanceof NullEncodingTwoWayFieldBridge) {
            fieldBridge = ((NullEncodingTwoWayFieldBridge) fieldBridge).unwrap();
         }
         // Determine sort type based on FieldBridgeType. SortField.BYTE and SortField.SHORT are not covered yet!
         if (fieldBridge instanceof NumericFieldBridge) {
            switch ((NumericFieldBridge) fieldBridge) {
               case INT_FIELD_BRIDGE:
                  sortType = SortField.Type.INT;
                  break;
               case LONG_FIELD_BRIDGE:
                  sortType = SortField.Type.LONG;
                  break;
               case FLOAT_FIELD_BRIDGE:
                  sortType = SortField.Type.FLOAT;
                  break;
               case DOUBLE_FIELD_BRIDGE:
                  sortType = SortField.Type.DOUBLE;
                  break;
            }
         }
         fields[i] = new SortField(sf.getPath().asStringPath(), sortType, !sf.isAscending());
      }

      return new Sort(fields);
   }

   @Override
   public Query visit(FullTextOccurExpr fullTextOccurExpr) {
      Query child = fullTextOccurExpr.getChild().acceptVisitor(this);
      return new BooleanQuery.Builder()
            .add(child, convertOccur(fullTextOccurExpr))  //TODO [anistor] the parent should 'absorb' this sub-expression to avoid the superfluous single-child BooleanQuery
            .build();
   }

   private BooleanClause.Occur convertOccur(FullTextOccurExpr fullTextOccurExpr) {
      switch (fullTextOccurExpr.getOccur()) {
         case SHOULD:
            return BooleanClause.Occur.SHOULD;
         case MUST:
            return BooleanClause.Occur.MUST;
         case MUST_NOT:
            return BooleanClause.Occur.MUST_NOT;
         case FILTER:
            return BooleanClause.Occur.FILTER;
      }
      throw new IllegalArgumentException("Unknown boolean occur value: " + fullTextOccurExpr.getOccur());
   }

   @Override
   public Query visit(FullTextBoostExpr fullTextBoostExpr) {
      Query child = fullTextBoostExpr.getChild().acceptVisitor(this);
      return new BoostQuery(child, fullTextBoostExpr.getBoost());
   }

   private boolean isMultiTermText(String fieldName, String text) {
      int terms = 0;
      try (TokenStream stream = entityAnalyzer.tokenStream(fieldName, new StringReader(text))) {
         CharTermAttribute attribute = stream.addAttribute(CharTermAttribute.class);
         stream.reset();
         while (stream.incrementToken()) {
            if (attribute.length() > 0) {
               if (++terms > 1) {
                  return true;
               }
            }
         }
         stream.end();
      } catch (IOException e) {
         // Highly unlikely when reading from a StreamReader.
         log.error(e);
      }
      return terms > 1;
   }

   @Override
   public Query visit(FullTextTermExpr fullTextTermExpr) {
      PropertyValueExpr propertyValueExpr = (PropertyValueExpr) fullTextTermExpr.getChild();
      String text = fullTextTermExpr.getTerm();

      int asteriskPos = text.indexOf(LUCENE_MULTIPLE_CHARACTERS_WILDCARD);
      int questionPos = text.indexOf(LUCENE_SINGLE_CHARACTER_WILDCARD);

      if (asteriskPos == -1 && questionPos == -1) {
         if (isMultiTermText(propertyValueExpr.getPropertyPath().asStringPath(), text)) {
            // phrase query
            PhraseContext phrase = queryBuilder.phrase();
            if (fullTextTermExpr.getFuzzySlop() != null) {
               phrase = phrase.withSlop(fullTextTermExpr.getFuzzySlop());
            }
            return phrase.onField(propertyValueExpr.getPropertyPath().asStringPath()).sentence(text).createQuery();
         } else {
            // just a single term
            if (fullTextTermExpr.getFuzzySlop() != null) {
               // fuzzy query
               return applyFieldBridge(propertyValueExpr.getPropertyPath(), queryBuilder.keyword()
                     .fuzzy().withEditDistanceUpTo(fullTextTermExpr.getFuzzySlop())
                     .onField(propertyValueExpr.getPropertyPath().asStringPath()))
                     .matching(text).createQuery();
            }
            // term query
            return applyFieldBridge(propertyValueExpr.getPropertyPath(), queryBuilder.keyword().onField(propertyValueExpr.getPropertyPath().asStringPath()))
                  .matching(text).createQuery();
         }
      } else {
         if (fullTextTermExpr.getFuzzySlop() != null) {
            throw log.getPrefixWildcardOrRegexpQueriesCannotBeFuzzy(fullTextTermExpr.toQueryString());
         }

         if (questionPos == -1 && asteriskPos == text.length() - 1) {
            // term prefix query
            String prefix = text.substring(0, text.length() - 1);
            return new PrefixQuery(new Term(propertyValueExpr.getPropertyPath().asStringPath(), prefix));
         }

         // wildcard query
         return applyFieldBridge(propertyValueExpr.getPropertyPath(), queryBuilder.keyword().wildcard().onField(propertyValueExpr.getPropertyPath().asStringPath()))
               .matching(text).createQuery();
      }
   }

   @Override
   public Query visit(FullTextRegexpExpr fullTextRegexpExpr) {
      PropertyValueExpr propertyValueExpr = (PropertyValueExpr) fullTextRegexpExpr.getChild();
      String regexp = fullTextRegexpExpr.getRegexp();

      // regexp query
      return new RegexpQuery(new Term(propertyValueExpr.getPropertyPath().asStringPath(), regexp));  //todo [anistor] fieldbridge?
   }

   @Override
   public Query visit(FullTextRangeExpr fullTextRangeExpr) {
      PropertyValueExpr propertyValueExpr = (PropertyValueExpr) fullTextRangeExpr.getChild();
      //todo [anistor] incomplete implementation ?
      if (fullTextRangeExpr.getLower() == null && fullTextRangeExpr.getUpper() == null) {
         return new TermRangeQuery(propertyValueExpr.getPropertyPath().asStringPath(), null, null, fullTextRangeExpr.isIncludeLower(), fullTextRangeExpr.isIncludeUpper());
      }
      RangeMatchingContext rangeMatchingContext = applyFieldBridge(propertyValueExpr.getPropertyPath(), queryBuilder.range().onField(propertyValueExpr.getPropertyPath().asStringPath()));
      RangeTerminationExcludable t = null;
      if (fullTextRangeExpr.getLower() != null) {
         t = rangeMatchingContext.above(fullTextRangeExpr.getLower());
         if (!fullTextRangeExpr.isIncludeLower()) {
            t.excludeLimit();
         }
      }
      if (fullTextRangeExpr.getUpper() != null) {
         t = rangeMatchingContext.below(fullTextRangeExpr.getUpper());
         if (!fullTextRangeExpr.isIncludeUpper()) {
            t.excludeLimit().createQuery();
         }
      }
      return t.createQuery();
   }

   @Override
   public Query visit(NotExpr notExpr) {
      Query transformedChild = notExpr.getChild().acceptVisitor(this);
      return queryBuilder.bool().must(transformedChild).not().createQuery();
   }

   @Override
   public Query visit(OrExpr orExpr) {
      BooleanJunction<BooleanJunction> booleanJunction = queryBuilder.bool();
      for (BooleanExpr c : orExpr.getChildren()) {
         Query transformedChild = c.acceptVisitor(this);
         booleanJunction.should(transformedChild);
      }
      return booleanJunction.createQuery();
   }

   @Override
   public Query visit(AndExpr andExpr) {
      BooleanJunction<BooleanJunction> booleanJunction = queryBuilder.bool();
      for (BooleanExpr c : andExpr.getChildren()) {
         if (c instanceof NotExpr) {
            // minor optimization: unwrap negated predicates and add child directly to this predicate
            BooleanExpr child = ((NotExpr) c).getChild();
            Query transformedChild = child.acceptVisitor(this);
            booleanJunction.must(transformedChild).not();
         } else {
            Query transformedChild = c.acceptVisitor(this);
            booleanJunction.must(transformedChild);
         }
      }
      return booleanJunction.createQuery();
   }

   @Override
   public Query visit(IsNullExpr isNullExpr) {
      PropertyValueExpr propertyValueExpr = (PropertyValueExpr) isNullExpr.getChild();
      return applyFieldBridge(propertyValueExpr.getPropertyPath(),
            queryBuilder.keyword().onField(propertyValueExpr.getPropertyPath().asStringPath())).matching(null).createQuery();
   }

   @Override
   public Query visit(ComparisonExpr comparisonExpr) {
      PropertyValueExpr propertyValueExpr = (PropertyValueExpr) comparisonExpr.getLeftChild();
      ConstantValueExpr constantValueExpr = (ConstantValueExpr) comparisonExpr.getRightChild();
      Comparable value = constantValueExpr.getConstantValueAs(propertyValueExpr.getPrimitiveType(), namedParameters);
      switch (comparisonExpr.getComparisonType()) {
         case NOT_EQUAL:
            Query q = applyFieldBridge(propertyValueExpr.getPropertyPath(), queryBuilder.keyword().onField(propertyValueExpr.getPropertyPath().asStringPath()))
                  .matching(value).createQuery();
            return queryBuilder.bool().must(q).not().createQuery();
         case EQUAL:
            return applyFieldBridge(propertyValueExpr.getPropertyPath(), queryBuilder.keyword().onField(propertyValueExpr.getPropertyPath().asStringPath()))
                  .matching(value).createQuery();
         case LESS:
            return applyFieldBridge(propertyValueExpr.getPropertyPath(), queryBuilder.range().onField(propertyValueExpr.getPropertyPath().asStringPath()))
                  .below(value).excludeLimit().createQuery();
         case LESS_OR_EQUAL:
            return applyFieldBridge(propertyValueExpr.getPropertyPath(), queryBuilder.range().onField(propertyValueExpr.getPropertyPath().asStringPath()))
                  .below(value).createQuery();
         case GREATER:
            return applyFieldBridge(propertyValueExpr.getPropertyPath(), queryBuilder.range().onField(propertyValueExpr.getPropertyPath().asStringPath()))
                  .above(value).excludeLimit().createQuery();
         case GREATER_OR_EQUAL:
            return applyFieldBridge(propertyValueExpr.getPropertyPath(), queryBuilder.range().onField(propertyValueExpr.getPropertyPath().asStringPath()))
                  .above(value).createQuery();
         default:
            throw new IllegalStateException("Unexpected comparison type: " + comparisonExpr.getComparisonType());
      }
   }

   @Override
   public Query visit(BetweenExpr betweenExpr) {
      PropertyValueExpr propertyValueExpr = (PropertyValueExpr) betweenExpr.getLeftChild();
      ConstantValueExpr fromValueExpr = (ConstantValueExpr) betweenExpr.getFromChild();
      ConstantValueExpr toValueExpr = (ConstantValueExpr) betweenExpr.getToChild();
      Comparable fromValue = fromValueExpr.getConstantValueAs(propertyValueExpr.getPrimitiveType(), namedParameters);
      Comparable toValue = toValueExpr.getConstantValueAs(propertyValueExpr.getPrimitiveType(), namedParameters);
      return applyFieldBridge(propertyValueExpr.getPropertyPath(), queryBuilder.range().onField(propertyValueExpr.getPropertyPath().asStringPath()))
            .from(fromValue).to(toValue).createQuery();
   }

   @Override
   public Query visit(LikeExpr likeExpr) {
      PropertyValueExpr propertyValueExpr = (PropertyValueExpr) likeExpr.getChild();
      StringBuilder lucenePattern = new StringBuilder(likeExpr.getPattern());
      // transform 'Like' pattern into Lucene wildcard pattern
      boolean isEscaped = false;
      for (int i = 0; i < lucenePattern.length(); i++) {
         char c = lucenePattern.charAt(i);
         if (!isEscaped && c == likeExpr.getEscapeChar()) {
            isEscaped = true;
            lucenePattern.deleteCharAt(i);
         } else {
            if (isEscaped) {
               isEscaped = false;
            } else {
               if (c == LikeExpr.MULTIPLE_CHARACTERS_WILDCARD) {
                  lucenePattern.setCharAt(i, LUCENE_MULTIPLE_CHARACTERS_WILDCARD);
                  continue;
               } else if (c == LikeExpr.SINGLE_CHARACTER_WILDCARD) {
                  lucenePattern.setCharAt(i, LUCENE_SINGLE_CHARACTER_WILDCARD);
                  continue;
               }
            }
            if (c == LUCENE_SINGLE_CHARACTER_WILDCARD || c == LUCENE_MULTIPLE_CHARACTERS_WILDCARD) {
               lucenePattern.insert(i, LUCENE_WILDCARD_ESCAPE_CHARACTER);
               i++;
            }
         }
      }
      return applyFieldBridge(propertyValueExpr.getPropertyPath(), queryBuilder.keyword().wildcard().onField(propertyValueExpr.getPropertyPath().asStringPath()))
            .matching(lucenePattern.toString()).createQuery();
   }

   @Override
   public Query visit(ConstantBooleanExpr constantBooleanExpr) {
      Query all = queryBuilder.all().createQuery();
      return constantBooleanExpr.getValue() ? all : queryBuilder.bool().must(all).not().createQuery();
   }

   @Override
   public Query visit(ConstantValueExpr constantValueExpr) {
      throw new IllegalStateException("This node type should not be visited");
   }

   @Override
   public Query visit(PropertyValueExpr propertyValueExpr) {
      throw new IllegalStateException("This node type should not be visited");
   }

   @Override
   public Query visit(AggregationExpr aggregationExpr) {
      throw new IllegalStateException("This node type should not be visited");
   }

   private <F extends FieldCustomization> F applyFieldBridge(PropertyPath<?> propertyPath, F f) {
      FieldBridge fieldBridge = fieldBridgeProvider.getFieldBridge(entityType, propertyPath.asArrayPath());
      if (fieldBridge != null) {
         ((FieldBridgeCustomization) f).withFieldBridge(fieldBridge);
         f.ignoreAnalyzer();
      }
      return f;
   }
}
