package org.infinispan.query.dsl.embedded.impl;

import static org.infinispan.query.dsl.embedded.impl.HibernateSearchPropertyHelper.KEY;
import static org.infinispan.query.dsl.embedded.impl.HibernateSearchPropertyHelper.VALUE;
import static org.infinispan.query.logging.Log.CONTAINER;

import java.io.IOException;
import java.io.StringReader;
import java.util.Map;
import java.util.Optional;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.RegexpQuery;
import org.hibernate.search.backend.lucene.LuceneBackend;
import org.hibernate.search.backend.lucene.LuceneExtension;
import org.hibernate.search.backend.lucene.search.predicate.dsl.LuceneSearchPredicateFactory;
import org.hibernate.search.engine.backend.metamodel.IndexFieldDescriptor;
import org.hibernate.search.engine.backend.metamodel.IndexValueFieldDescriptor;
import org.hibernate.search.engine.search.predicate.SearchPredicate;
import org.hibernate.search.engine.search.predicate.dsl.BooleanPredicateClausesStep;
import org.hibernate.search.engine.search.predicate.dsl.ExistsPredicateOptionsStep;
import org.hibernate.search.engine.search.predicate.dsl.MatchPredicateOptionsStep;
import org.hibernate.search.engine.search.predicate.dsl.PhrasePredicateOptionsStep;
import org.hibernate.search.engine.search.predicate.dsl.PredicateBoostStep;
import org.hibernate.search.engine.search.predicate.dsl.PredicateFinalStep;
import org.hibernate.search.engine.search.predicate.dsl.RangePredicateFieldMoreStep;
import org.hibernate.search.engine.search.predicate.dsl.SimpleQueryFlag;
import org.hibernate.search.engine.search.projection.SearchProjection;
import org.hibernate.search.engine.search.projection.dsl.SearchProjectionFactory;
import org.hibernate.search.engine.search.sort.SearchSort;
import org.hibernate.search.engine.search.sort.dsl.CompositeSortComponentsStep;
import org.hibernate.search.engine.search.sort.dsl.FieldSortOptionsStep;
import org.hibernate.search.engine.search.sort.dsl.SearchSortFactory;
import org.hibernate.search.util.common.data.RangeBoundInclusion;
import org.infinispan.objectfilter.SortField;
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
import org.infinispan.objectfilter.impl.syntax.parser.IckleParsingResult;
import org.infinispan.query.logging.Log;
import org.infinispan.search.mapper.common.EntityReference;
import org.infinispan.search.mapper.mapping.SearchIndexedEntity;
import org.infinispan.search.mapper.mapping.SearchMapping;
import org.infinispan.search.mapper.scope.SearchScope;
import org.infinispan.search.mapper.session.SearchSession;
import org.jboss.logging.Logger;

/**
 * An *Expr {@link Visitor} that transforms a {@link IckleParsingResult} into a {@link SearchQueryParsingResult}.
 * <p>
 * NOTE: This is not stateless, not threadsafe, so it can only be used for a single transformation at a time.
 *
 * @author anistor@redhat.com
 * @author Fabio Massimo Ercoli
 * @since 9.0
 */
public final class SearchQueryMaker<TypeMetadata> implements Visitor<PredicateFinalStep, PredicateFinalStep> {

   private static final Log log = Logger.getMessageLogger(Log.class, SearchQueryMaker.class.getName());

   private static final char LUCENE_SINGLE_CHARACTER_WILDCARD = '?';
   private static final char LUCENE_MULTIPLE_CHARACTERS_WILDCARD = '*';
   private static final char LUCENE_WILDCARD_ESCAPE_CHARACTER = '\\';

   private final SearchMapping searchMapping;

   private Map<String, Object> namedParameters;
   private LuceneSearchPredicateFactory predicateFactory;
   private SearchIndexedEntity indexedEntity;

   SearchQueryMaker(SearchMapping searchMapping) {
      this.searchMapping = searchMapping;
   }

   public SearchQueryParsingResult transform(IckleParsingResult<TypeMetadata> parsingResult, Map<String, Object> namedParameters,
                                             Class<?> targetedType, String targetedNamedType) {
      if (searchMapping == null) {
         throw log.noTypeIsIndexed(parsingResult.getQueryString());
      }

      this.namedParameters = namedParameters;

      SearchSession querySession = searchMapping.getMappingSession();
      SearchScope<?> scope = (targetedNamedType == null) ? querySession.scope(targetedType) :
            querySession.scope(targetedType, targetedNamedType);

      predicateFactory = scope.predicate().extension(LuceneExtension.get());

      indexedEntity = (targetedNamedType == null) ? searchMapping.indexedEntity(targetedType) :
            searchMapping.indexedEntity(targetedNamedType);

      SearchPredicate predicate = makePredicate(parsingResult.getWhereClause()).toPredicate();
      SearchProjectionInfo projection = makeProjection(scope.projection(), parsingResult.getProjections(),
            parsingResult.getProjectedTypes());
      SearchSort sort = makeSort(scope.sort(), parsingResult.getSortFields());

      return new SearchQueryParsingResult(targetedType, targetedNamedType, projection, predicate, sort);
   }

   private SearchProjectionInfo makeProjection(SearchProjectionFactory<EntityReference, ?> projectionFactory,
                                               String[] projections, Class<?>[] projectedTypes) {
      if (projections == null || projections.length == 0) {
         return SearchProjectionInfo.entity(projectionFactory);
      }

      if (projections.length == 1) {
         if (VALUE.equals(projections[0])) {
            return SearchProjectionInfo.entity(projectionFactory);
         }
         if (KEY.equals(projections[0])) {
            return SearchProjectionInfo.entityReference(projectionFactory);
         }
         return SearchProjectionInfo.field(projectionFactory, projections[0], projectedTypes[0]);
      }

      SearchProjection<?>[] searchProjections = new SearchProjection<?>[projections.length];
      for (int i = 0; i < projections.length; i++) {
         if (VALUE.equals(projections[i])) {
            searchProjections[i] = projectionFactory.entity().toProjection();
         } else if (KEY.equals(projections[i])) {
            searchProjections[i] = projectionFactory.entityReference().toProjection();
         } else {
            searchProjections[i] = projectionFactory.field(projections[i], projectedTypes[i]).toProjection();
         }
      }
      return SearchProjectionInfo.composite(projectionFactory, searchProjections);
   }

   private SearchSort makeSort(SearchSortFactory sortFactory, SortField[] sortFields) {
      if (sortFields == null || sortFields.length == 0) {
         return sortFactory.score().toSort();
      }
      if (sortFields.length == 1) {
         return makeSort(sortFactory, sortFields[0]);
      }
      CompositeSortComponentsStep<?> composite = sortFactory.composite();
      for (SortField sortField : sortFields) {
         composite.add(makeSort(sortFactory, sortField));
      }
      return composite.toSort();
   }

   private SearchSort makeSort(SearchSortFactory sortFactory, SortField sortField) {
      FieldSortOptionsStep<?, ?> optionsStep = sortFactory.field(sortField.getPath().asStringPathWithoutAlias());
      if (sortField.isAscending()) {
         optionsStep.asc();
      } else {
         optionsStep.desc();
      }
      return optionsStep.toSort();
   }

   private PredicateFinalStep makePredicate(BooleanExpr expr) {
      return expr == null ? predicateFactory.matchAll() : expr.acceptVisitor(this);
   }

   @Override
   public PredicateFinalStep visit(FullTextOccurExpr fullTextOccurExpr) {
      PredicateFinalStep childPredicate = fullTextOccurExpr.getChild().acceptVisitor(this);

      switch (fullTextOccurExpr.getOccur()) {
         case SHOULD:
            return predicateFactory.bool().should(childPredicate);
         case MUST:
         case FILTER:
            return predicateFactory.bool().must(childPredicate);
         case MUST_NOT:
            return predicateFactory.bool().mustNot(childPredicate);
      }
      throw new IllegalArgumentException("Unknown boolean occur clause: " + fullTextOccurExpr.getOccur());
   }

   @Override
   public PredicateFinalStep visit(FullTextBoostExpr fullTextBoostExpr) {
      BooleanExpr child = fullTextBoostExpr.getChild();
      float boost = fullTextBoostExpr.getBoost();

      // TODO HSEARCH-3884 Support regexp queries
      if (child instanceof FullTextRegexpExpr) {
         FullTextRegexpExpr fullTextRegexpExpr = (FullTextRegexpExpr) child;
         PropertyValueExpr propertyValueExpr = (PropertyValueExpr) fullTextRegexpExpr.getChild();
         String regexp = fullTextRegexpExpr.getRegexp();

         // boosting native queries
         RegexpQuery nativeRegexQuery = new RegexpQuery(new Term(propertyValueExpr.getPropertyPath().asStringPath(), regexp));
         BoostQuery nativeQuery = new BoostQuery(nativeRegexQuery, fullTextBoostExpr.getBoost());
         return predicateFactory.fromLuceneQuery(nativeQuery);
      }

      PredicateFinalStep childPredicate = child.acceptVisitor(this);
      if (childPredicate instanceof PredicateBoostStep) {
         ((PredicateBoostStep) childPredicate).boost(boost);
      }
      return childPredicate;
   }

   private boolean isMultiTermText(PropertyPath<?> propertyPath, String text) {
      Analyzer analyzer = getAnalyzer(propertyPath);
      if (analyzer == null) {
         analyzer = new WhitespaceAnalyzer();
      }

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

   private Analyzer getAnalyzer(PropertyPath<?> propertyPath) {
      Optional<IndexFieldDescriptor> indexFieldDescriptor =
            indexedEntity.indexManager().descriptor().field(propertyPath.asStringPath());
      if (!indexFieldDescriptor.isPresent()) {
         return null;
      }

      IndexFieldDescriptor fieldDescriptor = indexFieldDescriptor.get();
      if (fieldDescriptor.isObjectField()) {
         return null;
      }

      IndexValueFieldDescriptor valueField = fieldDescriptor.toValueField();
      Optional<String> analyzerName = valueField.type().analyzerName();
      if (!analyzerName.isPresent()) {
         return null;
      }

      LuceneBackend luceneBackend = indexedEntity.indexManager().backend().unwrap(LuceneBackend.class);
      Optional<? extends Analyzer> analyzer = luceneBackend.analyzer(analyzerName.get());
      if (!analyzer.isPresent()) {
         return null;
      }

      return analyzer.get();
   }

   @Override
   public PredicateFinalStep visit(FullTextTermExpr fullTextTermExpr) {
      PropertyValueExpr propertyValueExpr = (PropertyValueExpr) fullTextTermExpr.getChild();
      String text = fullTextTermExpr.getTerm();
      String absoluteFieldPath = propertyValueExpr.getPropertyPath().asStringPath();

      int asteriskPos = text.indexOf(LUCENE_MULTIPLE_CHARACTERS_WILDCARD);
      int questionPos = text.indexOf(LUCENE_SINGLE_CHARACTER_WILDCARD);

      if (asteriskPos == -1 && questionPos == -1) {
         if (isMultiTermText(propertyValueExpr.getPropertyPath(), text)) {
            PhrasePredicateOptionsStep<?> result = predicateFactory.phrase().field(absoluteFieldPath).matching(text);
            if (fullTextTermExpr.getFuzzySlop() != null) {
               // slop 4 phrase
               result.slop(fullTextTermExpr.getFuzzySlop());
            }
            return result;
         } else {
            MatchPredicateOptionsStep<?> result = predicateFactory.match().field(absoluteFieldPath).matching(text);
            if (fullTextTermExpr.getFuzzySlop() != null) {
               // fuzzy 4 match
               result.fuzzy(fullTextTermExpr.getFuzzySlop());
            }
            return result;
         }
      } else {
         if (fullTextTermExpr.getFuzzySlop() != null) {
            throw CONTAINER.getPrefixWildcardOrRegexpQueriesCannotBeFuzzy(fullTextTermExpr.toQueryString());
         }

         if (questionPos == -1 && asteriskPos == text.length() - 1) {
            // term prefix query
            // TODO HSEARCH-3906 use a term prefix predicate
            // even if simpleQueryString + prefix flag should work as well
            return predicateFactory.simpleQueryString()
                  .field(absoluteFieldPath).matching(text)
                  .flags(SimpleQueryFlag.PREFIX);
         }

         // wildcard query
         return predicateFactory.wildcard()
               .field(absoluteFieldPath).matching(text);
      }
   }

   @Override
   public PredicateFinalStep visit(FullTextRegexpExpr fullTextRegexpExpr) {
      PropertyValueExpr propertyValueExpr = (PropertyValueExpr) fullTextRegexpExpr.getChild();
      String regexp = fullTextRegexpExpr.getRegexp();

      // TODO TODO HSEARCH-3884 Support regexp queries
      RegexpQuery nativeQuery = new RegexpQuery(new Term(propertyValueExpr.getPropertyPath().asStringPath(), regexp));
      return predicateFactory.fromLuceneQuery(nativeQuery);
   }

   @Override
   public PredicateFinalStep visit(FullTextRangeExpr fullTextRangeExpr) {
      PropertyValueExpr propertyValueExpr = (PropertyValueExpr) fullTextRangeExpr.getChild();

      String absoluteFieldPath = propertyValueExpr.getPropertyPath().asStringPath();
      Object lower = fullTextRangeExpr.getLower();
      Object upper = fullTextRangeExpr.getUpper();
      boolean includeLower = fullTextRangeExpr.isIncludeLower();
      boolean includeUpper = fullTextRangeExpr.isIncludeUpper();

      if (lower == null && upper == null) {
         return predicateFactory.exists().field(absoluteFieldPath);
      }

      RangePredicateFieldMoreStep<?, ?> range = predicateFactory.range().field(absoluteFieldPath);
      return range.between(
            lower, includeLower ? RangeBoundInclusion.INCLUDED : RangeBoundInclusion.EXCLUDED,
            upper, includeUpper ? RangeBoundInclusion.INCLUDED : RangeBoundInclusion.EXCLUDED);
   }

   @Override
   public PredicateFinalStep visit(NotExpr notExpr) {
      BooleanExpr childExpr = notExpr.getChild();

      // not is null => exists
      // instead of not ( not ( exists ) )
      if (childExpr instanceof IsNullExpr) {
         PropertyValueExpr propertyValueExpr = (PropertyValueExpr) ((IsNullExpr) childExpr).getChild();
         return predicateFactory.exists().field(propertyValueExpr.getPropertyPath().asStringPath());
      }

      PredicateFinalStep childPredicate = childExpr.acceptVisitor(this);
      return predicateFactory.bool().mustNot(childPredicate);
   }

   @Override
   public PredicateFinalStep visit(OrExpr orExpr) {
      BooleanPredicateClausesStep<?> bool = predicateFactory.bool();
      for (BooleanExpr c : orExpr.getChildren()) {
         PredicateFinalStep clause = c.acceptVisitor(this);
         bool.should(clause);
      }
      return bool;
   }

   @Override
   public PredicateFinalStep visit(AndExpr andExpr) {
      BooleanPredicateClausesStep<?> bool = predicateFactory.bool();
      for (BooleanExpr c : andExpr.getChildren()) {
         PredicateFinalStep clause = c.acceptVisitor(this);
         bool.must(clause);
      }
      return bool;
   }

   @Override
   public PredicateFinalStep visit(IsNullExpr isNullExpr) {
      PropertyValueExpr propertyValueExpr = (PropertyValueExpr) isNullExpr.getChild();
      ExistsPredicateOptionsStep<?> exists = predicateFactory.exists().field(propertyValueExpr.getPropertyPath().asStringPath());
      return predicateFactory.bool().mustNot(exists);
   }

   @Override
   public PredicateFinalStep visit(ComparisonExpr comparisonExpr) {
      PropertyValueExpr propertyValueExpr = (PropertyValueExpr) comparisonExpr.getLeftChild();
      ConstantValueExpr constantValueExpr = (ConstantValueExpr) comparisonExpr.getRightChild();
      Comparable value = constantValueExpr.getConstantValueAs(propertyValueExpr.getPrimitiveType(), namedParameters);
      String path = propertyValueExpr.getPropertyPath().asStringPath();
      switch (comparisonExpr.getComparisonType()) {
         case NOT_EQUAL:
            return predicateFactory.bool().mustNot(c -> c.match().field(path).matching(value));
         case EQUAL:
            return predicateFactory.match().field(path).matching(value);
         case LESS:
            return predicateFactory.range().field(path).lessThan(value);
         case LESS_OR_EQUAL:
            return predicateFactory.range().field(path).atMost(value);
         case GREATER:
            return predicateFactory.range().field(path).greaterThan(value);
         case GREATER_OR_EQUAL:
            return predicateFactory.range().field(path).atLeast(value);
         default:
            throw new IllegalStateException("Unexpected comparison type: " + comparisonExpr.getComparisonType());
      }
   }

   @Override
   public PredicateFinalStep visit(BetweenExpr betweenExpr) {
      PropertyValueExpr propertyValueExpr = (PropertyValueExpr) betweenExpr.getLeftChild();
      String path = propertyValueExpr.getPropertyPath().asStringPath();

      ConstantValueExpr fromValueExpr = (ConstantValueExpr) betweenExpr.getFromChild();
      ConstantValueExpr toValueExpr = (ConstantValueExpr) betweenExpr.getToChild();
      Comparable fromValue = fromValueExpr.getConstantValueAs(propertyValueExpr.getPrimitiveType(), namedParameters);
      Comparable toValue = toValueExpr.getConstantValueAs(propertyValueExpr.getPrimitiveType(), namedParameters);

      return predicateFactory.range().field(path).between(fromValue, toValue);
   }

   @Override
   public PredicateFinalStep visit(LikeExpr likeExpr) {
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
      String path = propertyValueExpr.getPropertyPath().asStringPath();
      return predicateFactory.wildcard().field(path).matching(lucenePattern.toString());
   }

   @Override
   public PredicateFinalStep visit(ConstantBooleanExpr constantBooleanExpr) {
      return constantBooleanExpr.getValue() ? predicateFactory.matchAll() :
            predicateFactory.bool().mustNot(predicateFactory.matchAll());
   }

   @Override
   public PredicateFinalStep visit(ConstantValueExpr constantValueExpr) {
      throw new IllegalStateException("This node type should not be visited");
   }

   @Override
   public PredicateFinalStep visit(PropertyValueExpr propertyValueExpr) {
      throw new IllegalStateException("This node type should not be visited");
   }

   @Override
   public PredicateFinalStep visit(AggregationExpr aggregationExpr) {
      throw new IllegalStateException("This node type should not be visited");
   }
}
