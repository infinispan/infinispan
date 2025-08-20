package org.infinispan.query.dsl.embedded.impl;

import static org.infinispan.query.core.impl.Log.CONTAINER;
import static org.infinispan.query.dsl.embedded.impl.HibernateSearchPropertyHelper.KEY;
import static org.infinispan.query.dsl.embedded.impl.HibernateSearchPropertyHelper.SCORE;
import static org.infinispan.query.dsl.embedded.impl.HibernateSearchPropertyHelper.VALUE;

import java.io.IOException;
import java.io.StringReader;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.hibernate.search.backend.lucene.LuceneBackend;
import org.hibernate.search.backend.lucene.LuceneExtension;
import org.hibernate.search.backend.lucene.search.predicate.dsl.LuceneSearchPredicateFactory;
import org.hibernate.search.engine.backend.metamodel.IndexFieldDescriptor;
import org.hibernate.search.engine.backend.metamodel.IndexValueFieldDescriptor;
import org.hibernate.search.engine.common.EntityReference;
import org.hibernate.search.engine.search.aggregation.SearchAggregation;
import org.hibernate.search.engine.search.predicate.SearchPredicate;
import org.hibernate.search.engine.search.predicate.dsl.BooleanPredicateClausesStep;
import org.hibernate.search.engine.search.predicate.dsl.ExistsPredicateOptionsStep;
import org.hibernate.search.engine.search.predicate.dsl.KnnPredicateOptionsStep;
import org.hibernate.search.engine.search.predicate.dsl.KnnPredicateVectorStep;
import org.hibernate.search.engine.search.predicate.dsl.MatchPredicateOptionsStep;
import org.hibernate.search.engine.search.predicate.dsl.NestedPredicateClausesStep;
import org.hibernate.search.engine.search.predicate.dsl.PhrasePredicateOptionsStep;
import org.hibernate.search.engine.search.predicate.dsl.PredicateFinalStep;
import org.hibernate.search.engine.search.predicate.dsl.PredicateScoreStep;
import org.hibernate.search.engine.search.predicate.dsl.RangePredicateFieldMoreStep;
import org.hibernate.search.engine.search.predicate.dsl.SearchPredicateFactory;
import org.hibernate.search.engine.search.predicate.dsl.RegexpQueryFlag;
import org.hibernate.search.engine.search.predicate.dsl.SimpleQueryFlag;
import org.hibernate.search.engine.search.projection.SearchProjection;
import org.hibernate.search.engine.search.projection.dsl.DistanceToFieldProjectionValueStep;
import org.hibernate.search.engine.search.projection.dsl.FieldProjectionValueStep;
import org.hibernate.search.engine.search.projection.dsl.SearchProjectionFactory;
import org.hibernate.search.engine.search.sort.SearchSort;
import org.hibernate.search.engine.search.sort.dsl.CompositeSortComponentsStep;
import org.hibernate.search.engine.search.sort.dsl.DistanceSortOptionsStep;
import org.hibernate.search.engine.search.sort.dsl.FieldSortOptionsStep;
import org.hibernate.search.engine.search.sort.dsl.SearchSortFactory;
import org.hibernate.search.engine.spatial.DistanceUnit;
import org.hibernate.search.engine.spatial.GeoPoint;
import org.hibernate.search.engine.spatial.GeoPolygon;
import org.hibernate.search.util.common.data.RangeBoundInclusion;
import org.infinispan.query.objectfilter.SortField;
import org.infinispan.query.objectfilter.impl.ql.PropertyPath;
import org.infinispan.query.objectfilter.impl.syntax.AggregationExpr;
import org.infinispan.query.objectfilter.impl.syntax.AndExpr;
import org.infinispan.query.objectfilter.impl.syntax.BetweenExpr;
import org.infinispan.query.objectfilter.impl.syntax.BooleanExpr;
import org.infinispan.query.objectfilter.impl.syntax.ComparisonExpr;
import org.infinispan.query.objectfilter.impl.syntax.ConstantBooleanExpr;
import org.infinispan.query.objectfilter.impl.syntax.ConstantValueExpr;
import org.infinispan.query.objectfilter.impl.syntax.FullTextBoostExpr;
import org.infinispan.query.objectfilter.impl.syntax.FullTextOccurExpr;
import org.infinispan.query.objectfilter.impl.syntax.FullTextRangeExpr;
import org.infinispan.query.objectfilter.impl.syntax.FullTextRegexpExpr;
import org.infinispan.query.objectfilter.impl.syntax.FullTextTermExpr;
import org.infinispan.query.objectfilter.impl.syntax.IsNullExpr;
import org.infinispan.query.objectfilter.impl.syntax.KnnPredicate;
import org.infinispan.query.objectfilter.impl.syntax.LikeExpr;
import org.infinispan.query.objectfilter.impl.syntax.NestedExpr;
import org.infinispan.query.objectfilter.impl.syntax.NotExpr;
import org.infinispan.query.objectfilter.impl.syntax.OrExpr;
import org.infinispan.query.objectfilter.impl.syntax.PropertyValueExpr;
import org.infinispan.query.objectfilter.impl.syntax.SpatialWithinBoxExpr;
import org.infinispan.query.objectfilter.impl.syntax.SpatialWithinCircleExpr;
import org.infinispan.query.objectfilter.impl.syntax.SpatialWithinPolygonExpr;
import org.infinispan.query.objectfilter.impl.syntax.Visitor;
import org.infinispan.query.objectfilter.impl.syntax.parser.AggregationPropertyPath;
import org.infinispan.query.objectfilter.impl.syntax.parser.CacheValueAggregationPropertyPath;
import org.infinispan.query.objectfilter.impl.syntax.parser.FunctionPropertyPath;
import org.infinispan.query.objectfilter.impl.syntax.parser.IckleParsingResult;
import org.infinispan.query.objectfilter.impl.syntax.parser.ObjectPropertyHelper;
import org.infinispan.query.core.impl.Log;
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

   private static final Log log = Logger.getMessageLogger(MethodHandles.lookup(), Log.class, SearchQueryMaker.class.getName());

   private static final char LUCENE_SINGLE_CHARACTER_WILDCARD = '?';
   private static final char LUCENE_MULTIPLE_CHARACTERS_WILDCARD = '*';
   private static final char LUCENE_WILDCARD_ESCAPE_CHARACTER = '\\';

   private final SearchMapping searchMapping;
   private final ObjectPropertyHelper<TypeMetadata> propertyHelper;
   private final int maxResults;
   private final int hitCountAccuracy;

   private Map<String, Object> namedParameters;
   private LuceneSearchPredicateFactory<?> predicateFactory;
   private SearchIndexedEntity indexedEntity;
   private Integer knn;
   private BooleanExpr filteringClause;

   SearchQueryMaker(SearchMapping searchMapping, ObjectPropertyHelper<TypeMetadata> propertyHelper, int maxResults, int hitCountAccuracy) {
      this.searchMapping = searchMapping;
      this.propertyHelper = propertyHelper;
      this.maxResults = maxResults;
      this.hitCountAccuracy = hitCountAccuracy;
   }

   public SearchQueryParsingResult transform(IckleParsingResult<TypeMetadata> parsingResult, Map<String, Object> namedParameters,
                                             Class<?> targetedType, String targetedTypeName) {
      if (searchMapping == null) {
         throw log.noTypeIsIndexed(parsingResult.getQueryString());
      }

      this.namedParameters = namedParameters;

      SearchSession querySession = searchMapping.getMappingSession();
      SearchScope<?> scope = targetedTypeName == null ? querySession.scope(targetedType) :
            querySession.scope(targetedType, targetedTypeName);

      predicateFactory = scope.predicate().extension(LuceneExtension.get());

      indexedEntity = targetedTypeName == null ? searchMapping.indexedEntity(targetedType) :
            searchMapping.indexedEntity(targetedTypeName);

      filteringClause = parsingResult.getFilteringClause();
      InfinispanAggregation<?> aggregation = makeAggregation(scope, parsingResult);
      SearchPredicate predicate = makePredicate(parsingResult.getWhereClause(), aggregation).toPredicate();
      SearchProjectionInfo projection = makeProjection(parsingResult.getTargetEntityMetadata(), scope.projection(), parsingResult.getProjectedPaths(),
            parsingResult.getProjectedTypes(), aggregation);
      SearchSort sort = makeSort(scope.sort(), parsingResult.getSortFields());

      return new SearchQueryParsingResult(targetedType, targetedTypeName, projection, aggregation, predicate, sort, hitCountAccuracy, knn);
   }

   private <T> InfinispanAggregation makeAggregation(SearchScope<?> scope, IckleParsingResult<TypeMetadata> parsingResult) {
      PropertyPath[] groupBy = parsingResult.getGroupBy();
      if (groupBy == null || groupBy.length != 1) {
         return null;
      }

      AggregationPropertyPath aggregationPropertyPath = null;
      Class<T> projectedType = null;
      boolean displayGroupFirst = false;

      for (int i = 0; i < parsingResult.getProjectedPaths().length; i++) {
         PropertyPath projectedPath = parsingResult.getProjectedPaths()[i];
         if (projectedPath instanceof AggregationPropertyPath) {
            if (projectedType != null) {
               displayGroupFirst = true;
            }
            aggregationPropertyPath = (AggregationPropertyPath) projectedPath;
         } else if (Arrays.equals(groupBy[0].asArrayPath(), projectedPath.asArrayPath())) {
            projectedType = (Class<T>) parsingResult.getProjectedTypes()[i];
         }
      }
      if (aggregationPropertyPath == null || projectedType == null) {
         return null;
      }

      SearchAggregation<? extends Map<T, Long>> searchAggregation = scope.aggregation().terms()
            .field(groupBy[0].asStringPathWithoutAlias(), projectedType)
            .maxTermCount(Integer.MAX_VALUE).toAggregation();
      return new InfinispanAggregation(searchAggregation, aggregationPropertyPath, displayGroupFirst);
   }

   private SearchProjectionInfo makeProjection(TypeMetadata typeMetadata, SearchProjectionFactory<EntityReference, ?> projectionFactory,
                                               PropertyPath<?>[] projections, Class<?>[] projectedTypes, InfinispanAggregation aggregation) {
      if (projections == null || projections.length == 0 || aggregation != null) {
         return SearchProjectionInfo.entity(projectionFactory);
      }

      if (projections.length == 1) {
         String projection = projections[0].asStringPath();
         if (VALUE.equals(projection)) {
            return SearchProjectionInfo.entity(projectionFactory);
         }
         if (KEY.equals(projection)) {
            return SearchProjectionInfo.entityReference(projectionFactory);
         }
         boolean isRepeatedProperty = propertyHelper.isRepeatedProperty(typeMetadata, projections[0].asArrayPath());
         if (projections[0] instanceof FunctionPropertyPath) {
            FunctionPropertyPath<TypeMetadata> distance = (FunctionPropertyPath<TypeMetadata>) projections[0];
            DistanceToFieldProjectionValueStep<?, Double> projectionStep = projectionFactory.distance(projection, new GeoPoint() {
               @Override
               public double latitude() {
                  return (Double) distance.getArgs().get(0);
               }

               @Override
               public double longitude() {
                  return (Double) distance.getArgs().get(1);
               }
            });
            if (distance.getArgs().size() > 2) {
               String unit = (String) distance.getArgs().get(2);
               DistanceUnit distanceUnit = DistanceUnitHelper.distanceUnit(unit);
               projectionStep.unit(distanceUnit);
            }

            return SearchProjectionInfo.composite(projectionFactory, new SearchProjection[]{
                  isRepeatedProperty ? projectionStep.multi().toProjection() : projectionStep.toProjection()
            });
         }
         if (isRepeatedProperty) {
            return SearchProjectionInfo.multiField(projectionFactory, projection, projectedTypes[0]);
         } else {
            return SearchProjectionInfo.field(projectionFactory, projection, projectedTypes[0]);
         }
      }

      SearchProjection<?>[] searchProjections = new SearchProjection<?>[projections.length];
      for (int i = 0; i < projections.length; i++) {
         String projection = projections[i].asStringPath();
         if (VALUE.equals(projection)) {
            searchProjections[i] = projectionFactory.entity().toProjection();
         } else if (KEY.equals(projection)) {
            searchProjections[i] = projectionFactory.entityReference().toProjection();
         } else if (SCORE.equals(projections[i])) {
            searchProjections[i] = projectionFactory.score().toProjection();
         } else {
            boolean isMultiField = propertyHelper.isRepeatedProperty(typeMetadata, projections[i].asArrayPath());
            if (projections[i] instanceof FunctionPropertyPath) {
               FunctionPropertyPath<TypeMetadata> distance = (FunctionPropertyPath<TypeMetadata>) projections[i];
               DistanceToFieldProjectionValueStep<?, Double> projectionStep = projectionFactory.distance(projection, new GeoPoint() {
                  @Override
                  public double latitude() {
                     return (Double) distance.getArgs().get(0);
                  }

                  @Override
                  public double longitude() {
                     return (Double) distance.getArgs().get(1);
                  }
               });
               if (distance.getArgs().size() > 2) {
                  String unit = (String) distance.getArgs().get(2);
                  DistanceUnit distanceUnit = DistanceUnitHelper.distanceUnit(unit);
                  projectionStep.unit(distanceUnit);
               }
               searchProjections[i] = isMultiField ? projectionStep.multi().toProjection() : projectionStep.toProjection();
            } else {
               FieldProjectionValueStep<?, ?> projectionStep = projectionFactory.field(projection, projectedTypes[i]);
               searchProjections[i] = isMultiField ? projectionStep.multi().toProjection() : projectionStep.toProjection();
            }
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
      CompositeSortComponentsStep<?, ?> composite = sortFactory.composite();
      for (SortField sortField : sortFields) {
         composite.add(makeSort(sortFactory, sortField));
      }
      return composite.toSort();
   }

   private SearchSort makeSort(SearchSortFactory sortFactory, SortField sortField) {
      PropertyPath<?> path = sortField.getPath();
      if (path instanceof FunctionPropertyPath) {
         FunctionPropertyPath<?> functionPath = (FunctionPropertyPath<?>) path;
         Double lat = (Double) functionPath.getArgs().get(0);
         Double lon = (Double) functionPath.getArgs().get(1);
         DistanceSortOptionsStep<?, ?, ? extends SearchPredicateFactory> optionsStep = sortFactory.distance(functionPath.asStringPathWithoutAlias(), lat, lon);
         return (sortField.isAscending() ? optionsStep.asc() : optionsStep.desc()).toSort();
      }
      FieldSortOptionsStep<?, ?, ? extends SearchPredicateFactory> optionsStep = sortFactory.field(path.asStringPathWithoutAlias());
      return (sortField.isAscending() ? optionsStep.asc() : optionsStep.desc()).toSort();
   }

   private PredicateFinalStep makePredicate(BooleanExpr expr, InfinispanAggregation aggregation) {
      if (expr == null) {
         return predicateFactory.matchAll();
      }

      PredicateFinalStep predicateFinalStep = expr.acceptVisitor(this);
      if (aggregation == null || aggregation.propertyPath() == null || aggregation.propertyPath() instanceof CacheValueAggregationPropertyPath) {
         if (knn != null && !(expr instanceof KnnPredicate)) {
            throw log.booleanKnnPredicates();
         }
         return predicateFinalStep;
      }

      return predicateFactory.bool()
            .must(predicateFinalStep)
            .must(predicateFactory.exists().field(aggregation.propertyPath().asStringPath()));
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

      if (child instanceof FullTextRegexpExpr) {
         FullTextRegexpExpr fullTextRegexpExpr = (FullTextRegexpExpr) child;
         PropertyValueExpr propertyValueExpr = (PropertyValueExpr) fullTextRegexpExpr.getChild();

         return predicateFactory.regexp().field(propertyValueExpr.getPropertyPath().asStringPath())
               .matching(fullTextRegexpExpr.getRegexp())
               .flags(RegexpQueryFlag.INTERVAL,
                     RegexpQueryFlag.INTERSECTION, RegexpQueryFlag.ANY_STRING)
               .boost(boost);
      }

      PredicateFinalStep childPredicate = child.acceptVisitor(this);
      if (childPredicate instanceof PredicateScoreStep) {
         ((PredicateScoreStep) childPredicate).boost(boost);
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
      String text = fullTextTermExpr.getTerm(namedParameters);
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

      return predicateFactory.regexp()
            .field(propertyValueExpr.getPropertyPath().asStringPath())
            .matching(fullTextRegexpExpr.getRegexp());
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

      RangePredicateFieldMoreStep<?, ?, ?> range = predicateFactory.range().field(absoluteFieldPath);
      return range.between(
            lower, includeLower ? RangeBoundInclusion.INCLUDED : RangeBoundInclusion.EXCLUDED,
            upper, includeUpper ? RangeBoundInclusion.INCLUDED : RangeBoundInclusion.EXCLUDED);
   }

   @Override
   public PredicateFinalStep visit(KnnPredicate knnPredicate) {
      PropertyValueExpr propertyValueExpr = (PropertyValueExpr) knnPredicate.getChild();
      String absoluteFieldPath = propertyValueExpr.getPropertyPath().asStringPath();

      if (knn != null) {
         throw log.multipleKnnPredicates();
      }

      knn = knnPredicate.knn(namedParameters);
      if (knn == null) {
         knn = maxResults;
      }

      KnnPredicateVectorStep knnPredicateVector = predicateFactory.knn(knn).field(absoluteFieldPath);
      KnnPredicateOptionsStep knnPredicateOptions;
      if (knnPredicate.floats()) {
         knnPredicateOptions = knnPredicateVector.matching(knnPredicate.floatsArray(namedParameters));
      } else {
         knnPredicateOptions = knnPredicateVector.matching(knnPredicate.bytesArray(namedParameters));
      }

      if (filteringClause != null) {
         PredicateFinalStep predicateFinalStep = filteringClause.acceptVisitor(this);
         knnPredicateOptions.filter(predicateFinalStep);
      }
      return knnPredicateOptions;
   }

   @Override
   public PredicateFinalStep visit(SpatialWithinCircleExpr spatialWithinCircleExpr) {
      PropertyValueExpr propertyValueExpr = (PropertyValueExpr) spatialWithinCircleExpr.getLeftChild();
      String path = propertyValueExpr.getPropertyPath().asStringPath();

      ConstantValueExpr latValueExpr = (ConstantValueExpr) spatialWithinCircleExpr.getLatChild();
      ConstantValueExpr lonValueExpr = (ConstantValueExpr) spatialWithinCircleExpr.getLonChild();
      ConstantValueExpr radiusValueExpr = (ConstantValueExpr) spatialWithinCircleExpr.getRadiusChild();
      ConstantValueExpr unitValueExpr = spatialWithinCircleExpr.getUnitChild();

      Double latValue = (Double) latValueExpr.getConstantValueAs(Double.class, namedParameters);
      Double lonValue = (Double) lonValueExpr.getConstantValueAs(Double.class, namedParameters);
      Double radiusValue = (Double) radiusValueExpr.getConstantValueAs(Double.class, namedParameters);
      String unitValue = (String) unitValueExpr.getConstantValueAs(String.class, namedParameters);
      DistanceUnit distanceUnit = DistanceUnitHelper.distanceUnit(unitValue);

      return predicateFactory.spatial().within().field(path).circle(latValue, lonValue, radiusValue, distanceUnit);
   }

   @Override
   public PredicateFinalStep visit(SpatialWithinBoxExpr spatialWithinBoxExpr) {
      PropertyValueExpr propertyValueExpr = (PropertyValueExpr) spatialWithinBoxExpr.getLeftChild();
      String path = propertyValueExpr.getPropertyPath().asStringPath();

      ConstantValueExpr tlLatChild = (ConstantValueExpr) spatialWithinBoxExpr.getTlLatChild();
      ConstantValueExpr tlLonChild = (ConstantValueExpr) spatialWithinBoxExpr.getTlLonChild();
      ConstantValueExpr brLatChild = (ConstantValueExpr) spatialWithinBoxExpr.getBrLatChild();
      ConstantValueExpr brLonChild = (ConstantValueExpr) spatialWithinBoxExpr.getBrLonChild();

      Double tlLat = (Double) tlLatChild.getConstantValueAs(Double.class, namedParameters);
      Double tlLon = (Double) tlLonChild.getConstantValueAs(Double.class, namedParameters);
      Double brLat = (Double) brLatChild.getConstantValueAs(Double.class, namedParameters);
      Double brLon = (Double) brLonChild.getConstantValueAs(Double.class, namedParameters);

      return predicateFactory.spatial().within().field(path).boundingBox(tlLat, tlLon, brLat, brLon);
   }

   @Override
   public PredicateFinalStep visit(SpatialWithinPolygonExpr spatialWithinPolygonExpr) {
      PropertyValueExpr propertyValueExpr = (PropertyValueExpr) spatialWithinPolygonExpr.getLeftChild();
      String path = propertyValueExpr.getPropertyPath().asStringPath();
      List<GeoPoint> geoPoints = new ArrayList<>(spatialWithinPolygonExpr.getVector().stream()
            .map(c -> (String) c.getConstantValueAs(String.class, namedParameters))
            .map(s -> {
               String substring = s.substring(1, s.length() - 1);
               String[] split = substring.split(",");
               double lat = Double.parseDouble(split[0]);
               double lon = Double.parseDouble(split[1]);
               return GeoPoint.of(lat, lon);
            })
            .toList());
      if (!geoPoints.isEmpty()) {
         // Hibernate Search API requires
         geoPoints.add(geoPoints.get(0));
      }

      return predicateFactory.spatial().within().field(path).polygon(GeoPolygon.of(geoPoints));
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
      BooleanPredicateClausesStep<?, ?> bool = predicateFactory.bool();
      for (BooleanExpr c : orExpr.getChildren()) {
         PredicateFinalStep clause = c.acceptVisitor(this);
         bool.should(clause);
      }
      return bool;
   }

   @Override
   public PredicateFinalStep visit(AndExpr andExpr) {
      BooleanPredicateClausesStep<?, ?> bool = predicateFactory.bool();
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

   @Override
   public PredicateFinalStep visit(NestedExpr nestedExpr) {
      BooleanPredicateClausesStep<?, ?> bool = predicateFactory.bool();
      NestedPredicateClausesStep<?, ?> nested = predicateFactory.nested(nestedExpr.getNestedPath());
      for (BooleanExpr c : nestedExpr.getChildren()) {
         PredicateFinalStep clause = c.acceptVisitor(this);
         nested.add(clause);
      }
      bool.must(nested);
      return bool;
   }

}
