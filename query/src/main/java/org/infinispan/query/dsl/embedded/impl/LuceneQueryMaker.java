package org.infinispan.query.dsl.embedded.impl;

import java.io.IOException;
import java.io.StringReader;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermRangeQuery;
import org.hibernate.search.analyzer.impl.LuceneAnalyzerReference;
import org.hibernate.search.analyzer.spi.AnalyzerReference;
import org.hibernate.search.bridge.FieldBridge;
import org.hibernate.search.bridge.builtin.NumericFieldBridge;
import org.hibernate.search.bridge.builtin.impl.NullEncodingTwoWayFieldBridge;
import org.hibernate.search.engine.integration.impl.ExtendedSearchIntegrator;
import org.hibernate.search.query.dsl.BooleanJunction;
import org.hibernate.search.query.dsl.EntityContext;
import org.hibernate.search.query.dsl.FieldCustomization;
import org.hibernate.search.query.dsl.PhraseContext;
import org.hibernate.search.query.dsl.QueryBuilder;
import org.hibernate.search.query.dsl.QueryContextBuilder;
import org.hibernate.search.query.dsl.RangeMatchingContext;
import org.hibernate.search.query.dsl.RangeTerminationExcludable;
import org.hibernate.search.query.dsl.Unit;
import org.hibernate.search.query.dsl.impl.FieldBridgeCustomization;
import org.hibernate.search.spi.SearchIntegrator;
import org.hibernate.search.spi.impl.PojoIndexedTypeIdentifier;
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
import org.infinispan.objectfilter.impl.syntax.GeofiltExpr;
import org.infinispan.objectfilter.impl.syntax.IsNullExpr;
import org.infinispan.objectfilter.impl.syntax.LikeExpr;
import org.infinispan.objectfilter.impl.syntax.NotExpr;
import org.infinispan.objectfilter.impl.syntax.OrExpr;
import org.infinispan.objectfilter.impl.syntax.PropertyValueExpr;
import org.infinispan.objectfilter.impl.syntax.Visitor;
import org.infinispan.objectfilter.impl.syntax.parser.IckleParsingResult;
import org.infinispan.query.logging.Log;
import org.jboss.logging.Logger;

/**
 * An *Expr {@link Visitor} that transforms a {@link IckleParsingResult} into a {@link LuceneQueryParsingResult}.
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
   private final FieldBridgeAndAnalyzerProvider<TypeMetadata> fieldBridgeAndAnalyzerProvider;
   private final SearchIntegrator searchFactory;

   private Map<String, Object> namedParameters;
   private QueryBuilder queryBuilder;
   private TypeMetadata entityType;
   private Analyzer entityAnalyzer;

   /**
    * This provides some glue code for Hibernate Search. Implementations are different for embedded and remote use case.
    */
   public interface FieldBridgeAndAnalyzerProvider<TypeMetadata> {

      /**
       * Returns the field bridge to be applied when executing queries on the given property of the given entity type.
       *
       * @param typeMetadata the entity type hosting the given property; may either identify an actual Java type or a
       *                     virtual type managed by the given implementation; never {@code null}
       * @param propertyPath an array of strings denoting the property path; never {@code null}
       * @return the field bridge to be used for querying the given property; may be {@code null}
       */
      FieldBridge getFieldBridge(TypeMetadata typeMetadata, String[] propertyPath);

      /**
       * Get the analyzer to be used for a property.
       */
      Analyzer getAnalyzer(SearchIntegrator searchIntegrator, TypeMetadata typeMetadata, String[] propertyPath);

      /**
       * Populate the EntityContext with the analyzers that will be used for properties.
       *
       * @param parsingResult the parsed query
       * @param entityContext the entity context to populate
       */
      void overrideAnalyzers(IckleParsingResult<TypeMetadata> parsingResult, EntityContext entityContext);
   }

   LuceneQueryMaker(SearchIntegrator searchFactory, FieldBridgeAndAnalyzerProvider<TypeMetadata> fieldBridgeAndAnalyzerProvider) {
      if (searchFactory == null) {
         throw new IllegalArgumentException("searchFactory argument cannot be null");
      }
      this.fieldBridgeAndAnalyzerProvider = fieldBridgeAndAnalyzerProvider;
      this.queryContextBuilder = searchFactory.buildQueryBuilder();
      this.searchFactory = searchFactory;
   }

   public LuceneQueryParsingResult<TypeMetadata> transform(IckleParsingResult<TypeMetadata> parsingResult, Map<String, Object> namedParameters, Class<?> targetedType) {
      this.namedParameters = namedParameters;
      EntityContext entityContext = queryContextBuilder.forEntity(targetedType);
      fieldBridgeAndAnalyzerProvider.overrideAnalyzers(parsingResult, entityContext);
      queryBuilder = entityContext.get();
      entityType = parsingResult.getTargetEntityMetadata();
      AnalyzerReference analyzerReference = ((ExtendedSearchIntegrator) searchFactory).getAnalyzerReference(new PojoIndexedTypeIdentifier(targetedType));
      if (analyzerReference.is(LuceneAnalyzerReference.class)) {
         entityAnalyzer = analyzerReference.unwrap(LuceneAnalyzerReference.class).getAnalyzer();
      }
      Query query = makeQuery(parsingResult.getWhereClause());

      // an all negative top level boolean query is not allowed; needs a bit of rewriting
      if (query instanceof BooleanQuery) {
         BooleanQuery booleanQuery = (BooleanQuery) query;
         boolean allClausesAreMustNot = booleanQuery.clauses().stream().allMatch(c -> c.getOccur() == BooleanClause.Occur.MUST_NOT);
         if (allClausesAreMustNot) {
            //It is illegal to have only must-not queries, in this case we need to add a positive clause to match everything else.
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            for (BooleanClause clause : booleanQuery.clauses()) {
               builder.add(clause.getQuery(), BooleanClause.Occur.MUST_NOT);
            }
            builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.FILTER);
            query = builder.build();
         }
      }

      //todo [anistor] use the hibernate search sort dsl
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
         FieldBridge fieldBridge = fieldBridgeAndAnalyzerProvider.getFieldBridge(entityType, sf.getPath().asArrayPath());
         if (fieldBridge instanceof NullEncodingTwoWayFieldBridge) {
            fieldBridge = ((NullEncodingTwoWayFieldBridge) fieldBridge).unwrap(FieldBridge.class);
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
      throw new IllegalArgumentException("Unknown boolean occur clause: " + fullTextOccurExpr.getOccur());
   }

   @Override
   public Query visit(FullTextBoostExpr fullTextBoostExpr) {
      Query child = fullTextBoostExpr.getChild().acceptVisitor(this);
      return new BoostQuery(child, fullTextBoostExpr.getBoost());
   }

   private boolean isMultiTermText(PropertyPath<?> propertyPath, String text) {
      Analyzer analyzer = fieldBridgeAndAnalyzerProvider.getAnalyzer(searchFactory, entityType, propertyPath.asArrayPath());
      if (analyzer == null) {
         analyzer = entityAnalyzer;
      }

      if (analyzer != null) {
         int terms = 0;
         try (TokenStream tokenStream = analyzer.tokenStream(propertyPath.asStringPathWithoutAlias(), new StringReader(text))) {
            PositionIncrementAttribute posIncAtt = tokenStream.addAttribute(PositionIncrementAttribute.class);
            tokenStream.reset();
            while (tokenStream.incrementToken()) {
               if (posIncAtt.getPositionIncrement() > 0) {
                  if (++terms > 1) {
                     break;
                  }
               }
            }
            tokenStream.end();
         } catch (IOException e) {
            // Highly unlikely to happen when reading from a StringReader.
            log.error(e);
         }
         return terms > 1;
      }

      // fallback to good old indexOf
      return text.trim().indexOf(' ') != -1;
   }

   @Override
   public Query visit(FullTextTermExpr fullTextTermExpr) {
      PropertyValueExpr propertyValueExpr = (PropertyValueExpr) fullTextTermExpr.getChild();
      String text = fullTextTermExpr.getTerm();

      int asteriskPos = text.indexOf(LUCENE_MULTIPLE_CHARACTERS_WILDCARD);
      int questionPos = text.indexOf(LUCENE_SINGLE_CHARACTER_WILDCARD);

      if (asteriskPos == -1 && questionPos == -1) {
         if (isMultiTermText(propertyValueExpr.getPropertyPath(), text)) {
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
               return applyFieldBridge(true, propertyValueExpr.getPropertyPath(), queryBuilder.keyword()
                     .fuzzy().withEditDistanceUpTo(fullTextTermExpr.getFuzzySlop())
                     .onField(propertyValueExpr.getPropertyPath().asStringPath()))
                     .matching(text).createQuery();
            }
            // term query
            return applyFieldBridge(true, propertyValueExpr.getPropertyPath(), queryBuilder.keyword().onField(propertyValueExpr.getPropertyPath().asStringPath()))
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
         return applyFieldBridge(true, propertyValueExpr.getPropertyPath(), queryBuilder.keyword().wildcard().onField(propertyValueExpr.getPropertyPath().asStringPath()))
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
      RangeMatchingContext rangeMatchingContext = applyFieldBridge(true, propertyValueExpr.getPropertyPath(), queryBuilder.range().onField(propertyValueExpr.getPropertyPath().asStringPath()));
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
            t.excludeLimit();
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
      BooleanQuery.Builder builder = new BooleanQuery.Builder();
      for (BooleanExpr c : andExpr.getChildren()) {
         boolean isNegative = c instanceof NotExpr;
         if (isNegative) {
            // minor optimization: unwrap negated predicates and add child directly to this predicate
            c = ((NotExpr) c).getChild();
         }
         Query transformedChild = c.acceptVisitor(this);
         if (transformedChild instanceof BooleanQuery) {
            // child absorption
            BooleanQuery booleanQuery = (BooleanQuery) transformedChild;
            if (booleanQuery.clauses().size() == 1) {
               BooleanClause clause = booleanQuery.clauses().get(0);
               BooleanClause.Occur occur = clause.getOccur();
               if (isNegative) {
                  occur = occur == BooleanClause.Occur.MUST_NOT ? BooleanClause.Occur.MUST : BooleanClause.Occur.MUST_NOT;
               }
               builder.add(clause.getQuery(), occur);
            } else {
               builder.add(transformedChild, isNegative ? BooleanClause.Occur.MUST_NOT : BooleanClause.Occur.MUST);
            }
         } else {
            builder.add(transformedChild, isNegative ? BooleanClause.Occur.MUST_NOT : BooleanClause.Occur.MUST);
         }
      }
      return builder.build();
   }

   @Override
   public Query visit(IsNullExpr isNullExpr) {
      PropertyValueExpr propertyValueExpr = (PropertyValueExpr) isNullExpr.getChild();
      return applyFieldBridge(false, propertyValueExpr.getPropertyPath(),
            queryBuilder.keyword().onField(propertyValueExpr.getPropertyPath().asStringPath())).matching(null).createQuery();
   }

   @Override
   public Query visit(ComparisonExpr comparisonExpr) {
      PropertyValueExpr propertyValueExpr = (PropertyValueExpr) comparisonExpr.getLeftChild();
      ConstantValueExpr constantValueExpr = (ConstantValueExpr) comparisonExpr.getRightChild();
      Comparable value = constantValueExpr.getConstantValueAs(propertyValueExpr.getPrimitiveType(), namedParameters);
      switch (comparisonExpr.getComparisonType()) {
         case NOT_EQUAL:
            Query q = applyFieldBridge(false, propertyValueExpr.getPropertyPath(), queryBuilder.keyword().onField(propertyValueExpr.getPropertyPath().asStringPath()))
                  .matching(value).createQuery();
            return queryBuilder.bool().must(q).not().createQuery();
         case EQUAL:
            return applyFieldBridge(false, propertyValueExpr.getPropertyPath(), queryBuilder.keyword().onField(propertyValueExpr.getPropertyPath().asStringPath()))
                  .matching(value).createQuery();
         case LESS:
            return applyFieldBridge(false, propertyValueExpr.getPropertyPath(), queryBuilder.range().onField(propertyValueExpr.getPropertyPath().asStringPath()))
                  .below(value).excludeLimit().createQuery();
         case LESS_OR_EQUAL:
            return applyFieldBridge(false, propertyValueExpr.getPropertyPath(), queryBuilder.range().onField(propertyValueExpr.getPropertyPath().asStringPath()))
                  .below(value).createQuery();
         case GREATER:
            return applyFieldBridge(false, propertyValueExpr.getPropertyPath(), queryBuilder.range().onField(propertyValueExpr.getPropertyPath().asStringPath()))
                  .above(value).excludeLimit().createQuery();
         case GREATER_OR_EQUAL:
            return applyFieldBridge(false, propertyValueExpr.getPropertyPath(), queryBuilder.range().onField(propertyValueExpr.getPropertyPath().asStringPath()))
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
      return applyFieldBridge(false, propertyValueExpr.getPropertyPath(), queryBuilder.range().onField(propertyValueExpr.getPropertyPath().asStringPath()))
            .from(fromValue).to(toValue).createQuery();
   }

   @Override
   public Query visit(LikeExpr likeExpr) {
      PropertyValueExpr propertyValueExpr = (PropertyValueExpr) likeExpr.getChild();
      StringBuilder lucenePattern = new StringBuilder(likeExpr.getPattern(namedParameters));
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
      return applyFieldBridge(false, propertyValueExpr.getPropertyPath(), queryBuilder.keyword().wildcard().onField(propertyValueExpr.getPropertyPath().asStringPath()))
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

   @Override
   public Query visit(GeofiltExpr geofiltExpr) {
      // todo [anistor] FieldCustomization<SpatialMatchingContext> ? see SpatialMatchingContext
      return queryBuilder.spatial()
            .onField(geofiltExpr.getChild().getPropertyPath().asStringPath())
            .within(geofiltExpr.getRadius(), Unit.KM)
            .ofLatitude(geofiltExpr.getLatitude())
            .andLongitude(geofiltExpr.getLongitude())
            .createQuery();
   }

   private <F extends FieldCustomization> F applyFieldBridge(boolean isAnalyzed, PropertyPath<?> propertyPath, F field) {
      FieldBridge fieldBridge = fieldBridgeAndAnalyzerProvider.getFieldBridge(entityType, propertyPath.asArrayPath());
      if (fieldBridge != null) {
         ((FieldBridgeCustomization) field).withFieldBridge(fieldBridge);
      }
      if (!isAnalyzed) {
         field.ignoreAnalyzer();
      }
      return field;
   }
}
