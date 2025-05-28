package org.infinispan.objectfilter.impl.syntax.parser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.antlr.runtime.tree.Tree;
import org.infinispan.objectfilter.SortField;
import org.infinispan.objectfilter.impl.logging.Log;
import org.infinispan.objectfilter.impl.ql.AggregationFunction;
import org.infinispan.objectfilter.impl.ql.Function;
import org.infinispan.objectfilter.impl.ql.JoinType;
import org.infinispan.objectfilter.impl.ql.PropertyPath;
import org.infinispan.objectfilter.impl.ql.QueryRendererDelegate;
import org.infinispan.objectfilter.impl.syntax.ComparisonExpr;
import org.infinispan.objectfilter.impl.syntax.ConstantValueExpr;
import org.infinispan.objectfilter.impl.syntax.IndexedFieldProvider;
import org.infinispan.objectfilter.impl.syntax.SpatialWithinCircleExpr;
import org.infinispan.objectfilter.impl.syntax.parser.projection.CacheValuePropertyPath;
import org.infinispan.objectfilter.impl.syntax.parser.projection.ScorePropertyPath;
import org.infinispan.objectfilter.impl.syntax.parser.projection.VersionPropertyPath;
import org.jboss.logging.Logger;

/**
 * Transform the parsed tree into a {@link IckleParsingResult} containing the {@link
 * org.infinispan.objectfilter.impl.syntax.BooleanExpr}s representing the WHERE and HAVING clauses of the query.
 *
 * @author Adrian Nistor
 * @since 9.0
 */
final class QueryRendererDelegateImpl<TypeMetadata> implements QueryRendererDelegate<TypeDescriptor<TypeMetadata>> {

   private static final Log log = Logger.getMessageLogger(Log.class, QueryRendererDelegateImpl.class.getName());

   /**
    * Initial length for various internal growable arrays.
    */
   private static final int ARRAY_INITIAL_LENGTH = 5;

   protected enum Phase {
      SELECT,
      FROM,
      WHERE,
      GROUP_BY,
      HAVING,
      ORDER_BY
   }

   /**
    * The current parsing phase
    */
   Phase phase;

   boolean filtering;

   private IckleParsingResult.StatementType statementType;

   private String targetTypeName;

   private TypeMetadata targetEntityMetadata;

   private IndexedFieldProvider.FieldIndexingMetadata fieldIndexingMetadata;

   private PropertyPath<TypeDescriptor<TypeMetadata>> propertyPath;

   private PropertyPath<TypeDescriptor<TypeMetadata>> propertyPathBackup;

   private AggregationFunction aggregationFunction;

   private Function function;

   private List<Object> functionArgs;

   private final VirtualExpressionBuilder<TypeMetadata> expressionBuilder;

   /**
    * Persister space: keep track of aliases and entity names.
    */
   private final Map<String, String> aliasToEntityType = new HashMap<>();
   /**
    * Map containing alias as a key and propertyPath as value
    * Currently, only registerJoinAlias populates this, so it will only contain join aliases
    */
   private final Map<String, PropertyPath<TypeDescriptor<TypeMetadata>>> aliasToPropertyPath = new HashMap<>();

   private String alias;

   private final Map<String, Object> namedParameters = new HashMap<>(ARRAY_INITIAL_LENGTH);

   private final ObjectPropertyHelper<TypeMetadata> propertyHelper;

   private List<PropertyPath<TypeDescriptor<TypeMetadata>>> groupBy;

   private List<SortField> sortFields;

   private List<PropertyPath<TypeDescriptor<TypeMetadata>>> projections;

   private List<Class<?>> projectedTypes;

   private List<Object> projectedNullMarkers;

   private final String queryString;

   private boolean asteriskCount = false;

   private String unit;

   QueryRendererDelegateImpl(String queryString, ObjectPropertyHelper<TypeMetadata> propertyHelper) {
      this.queryString = queryString;
      this.propertyHelper = propertyHelper;
      this.expressionBuilder = new VirtualExpressionBuilder<>(this, propertyHelper, aliasToPropertyPath);
   }

   /**
    * See rule entityName
    */
   @Override
   public void registerPersisterSpace(String entityName, Tree aliasTree) {
      String aliasText = aliasTree.getText();
      String previous = aliasToEntityType.put(aliasText, entityName);
      if (previous != null && !previous.equalsIgnoreCase(entityName)) {
         throw new UnsupportedOperationException("Alias reuse currently not supported: alias " + aliasText + " already assigned to type " + previous);
      }
      if (targetTypeName != null) {
         throw new IllegalStateException("Can't target multiple types: " + targetTypeName + " already selected before " + entityName);
      }
      targetTypeName = entityName;
      targetEntityMetadata = propertyHelper.getEntityMetadata(targetTypeName);
      if (targetEntityMetadata == null) {
         throw log.getUnknownEntity(targetTypeName);
      }
      fieldIndexingMetadata = propertyHelper.getIndexedFieldProvider().get(targetEntityMetadata);
      expressionBuilder.setEntityType(targetEntityMetadata);
   }

   // TODO [anistor] unused ??
   public void registerEmbeddedAlias(String aliasText, PropertyPath<TypeDescriptor<TypeMetadata>> propertyPath) {
      PropertyPath<TypeDescriptor<TypeMetadata>> previous = aliasToPropertyPath.put(aliasText, propertyPath);
      if (previous != null) {
         throw new UnsupportedOperationException("Alias reuse currently not supported: alias " + aliasText + " already assigned to type " + previous);
      }
   }

   @Override
   public boolean isUnqualifiedPropertyReference() {
      return true;
   }

   @Override
   public boolean isPersisterReferenceAlias() {
      return aliasToEntityType.containsKey(alias);
   }

   @Override
   public void activateFromStrategy(JoinType joinType, Tree associationFetchTree, Tree propertyFetchTree, Tree alias) {
      phase = Phase.FROM;
      this.alias = alias.getText();
   }

   @Override
   public void activateSelectStrategy() {
      phase = Phase.SELECT;
      statementType = IckleParsingResult.StatementType.SELECT;
   }

   @Override
   public void activateDeleteStrategy() {
      phase = Phase.SELECT;  // Not a mistake, DELETE is parsed similarly to a SELECT
      statementType = IckleParsingResult.StatementType.DELETE;
   }

   @Override
   public void activateWhereStrategy() {
      phase = Phase.WHERE;
   }

   @Override
   public void activateGroupByStrategy() {
      phase = Phase.GROUP_BY;
   }

   @Override
   public void activateHavingStrategy() {
      phase = Phase.HAVING;
   }

   @Override
   public void activateOrderByStrategy() {
      phase = Phase.ORDER_BY;
   }

   @Override
   public void activateFiltering() {
      filtering = true;
      propertyPathBackup = propertyPath;
   }

   @Override
   public void deactivateFiltering() {
      filtering = false;
      propertyPath = propertyPathBackup;
   }

   @Override
   public void deactivateStrategy() {
      phase = null;
      alias = null;
      propertyPath = null;
      aggregationFunction = null;
      filtering = false;
      function = null;
      functionArgs = null;
   }

   @Override
   public void activateOR() {
      expressionBuilder.pushOr();
   }

   @Override
   public void activateAND() {
      expressionBuilder.pushAnd();
   }

   @Override
   public void activateNOT() {
      expressionBuilder.pushNot();
   }

   @Override
   public void predicateLess(String value) {
      addComparisonPredicate(value, ComparisonExpr.Type.LESS);
   }

   @Override
   public void predicateLessOrEqual(String value) {
      addComparisonPredicate(value, ComparisonExpr.Type.LESS_OR_EQUAL);
   }

   /**
    * This implements the equality predicate; the comparison
    * predicate could be a constant, a subfunction or
    * some random type parameter.
    * The tree node has all details but with current tree rendering
    * it just passes it's text so we have to figure out the options again.
    */
   @Override
   public void predicateEquals(String value) {
      addComparisonPredicate(value, ComparisonExpr.Type.EQUAL);
   }

   @Override
   public void predicateNotEquals(String value) {
      activateNOT();
      addComparisonPredicate(value, ComparisonExpr.Type.EQUAL);
      deactivateBoolean();
   }

   @Override
   public void predicateGreaterOrEqual(String value) {
      addComparisonPredicate(value, ComparisonExpr.Type.GREATER_OR_EQUAL);
   }

   @Override
   public void predicateGreater(String value) {
      addComparisonPredicate(value, ComparisonExpr.Type.GREATER);
   }

   private void addComparisonPredicate(String value, ComparisonExpr.Type comparisonType) {
      ensureLeftSideIsAPropertyPath();
      PropertyPath<TypeDescriptor<TypeMetadata>> property = resolveAlias(propertyPath);
      if (property.isEmpty()) {
         throw log.getPredicatesOnEntityAliasNotAllowedException(propertyPath.asStringPath());
      }
      String rootAlias = propertyPath.getAlias();
      Object comparisonValue = parameterValue(value);
      checkAnalyzed(property, false);
      expressionBuilder.addComparison(property, comparisonType, comparisonValue, rootAlias);
   }

   @Override
   public void predicateIn(List<String> list) {
      ensureLeftSideIsAPropertyPath();
      List<Object> values = new ArrayList<>(list.size());
      for (String string : list) {
         values.add(parameterValue(string));
      }
      PropertyPath<TypeDescriptor<TypeMetadata>> property = resolveAlias(propertyPath);
      if (property.isEmpty()) {
         throw log.getPredicatesOnEntityAliasNotAllowedException(propertyPath.asStringPath());
      }
      expressionBuilder.addIn(property, values);
   }

   @Override
   public void predicateBetween(String lowerValue, String upperValue) {
      ensureLeftSideIsAPropertyPath();
      Object lowerComparisonValue = parameterValue(lowerValue);
      Object upperComparisonValue = parameterValue(upperValue);
      PropertyPath<TypeDescriptor<TypeMetadata>> property = resolveAlias(propertyPath);
      if (property.isEmpty()) {
         throw log.getPredicatesOnEntityAliasNotAllowedException(propertyPath.asStringPath());
      }
      checkAnalyzed(property, false);
      expressionBuilder.addRange(property, lowerComparisonValue, upperComparisonValue);
   }

   @Override
   public void predicateLike(String patternValue, Character escapeCharacter) {
      ensureLeftSideIsAPropertyPath();
      Object pattern = parameterValue(patternValue);
      PropertyPath<TypeDescriptor<TypeMetadata>> property = resolveAlias(propertyPath);
      if (property.isEmpty()) {
         throw log.getPredicatesOnEntityAliasNotAllowedException(propertyPath.asStringPath());
      }
      checkAnalyzed(property, false);
      expressionBuilder.addLike(property, pattern, escapeCharacter);
   }

   @Override
   public void predicateIsNull() {
      ensureLeftSideIsAPropertyPath();
      PropertyPath<TypeDescriptor<TypeMetadata>> property = resolveAlias(propertyPath);
      if (property.isEmpty()) {
         throw log.getPredicatesOnEntityAliasNotAllowedException(propertyPath.asStringPath());
      }
      checkAnalyzed(property, false);
      expressionBuilder.addIsNull(property);
   }

   @Override
   public void predicateConstantBoolean(boolean booleanConstant) {
      expressionBuilder.addConstantBoolean(booleanConstant);
   }

   @Override
   public void predicateFullTextTerm(String term, String fuzzyFlop) {
      ensureLeftSideIsAPropertyPath();
      PropertyPath<TypeDescriptor<TypeMetadata>> property = resolveAlias(propertyPath);
      if (property.isEmpty()) {
         throw log.getPredicatesOnEntityAliasNotAllowedException(propertyPath.asStringPath());
      }

      checkAnalyzed(property, true);
      Object comparisonObject = parameterValue(term);
      Integer fuzzy = fuzzyFlop == null ? null : ("~".equals(fuzzyFlop) ? 2 : Integer.parseInt(fuzzyFlop));
      expressionBuilder.addFullTextTerm(property, comparisonObject, fuzzy);
   }

   @Override
   public void predicateFullTextRegexp(String term) {
      ensureLeftSideIsAPropertyPath();
      PropertyPath<TypeDescriptor<TypeMetadata>> property = resolveAlias(propertyPath);
      if (property.isEmpty()) {
         throw log.getPredicatesOnEntityAliasNotAllowedException(propertyPath.asStringPath());
      }
      checkAnalyzed(property, true);
      expressionBuilder.addFullTextRegexp(property, term);
   }

   @Override
   public void predicateFullTextRange(boolean includeLower, String lower, String upper, boolean includeUpper) {
      ensureLeftSideIsAPropertyPath();
      PropertyPath<TypeDescriptor<TypeMetadata>> property = resolveAlias(propertyPath);
      if (property.isEmpty()) {
         throw log.getPredicatesOnEntityAliasNotAllowedException(propertyPath.asStringPath());
      }
      Object from = lower != null ? parameterValue(lower) : null;
      Object to = upper != null ? parameterValue(upper) : null;
      checkIndexed(property);
      expressionBuilder.addFullTextRange(property, includeLower, from, to, includeUpper);
   }

   @Override
   public void predicateKNN(List<String> vectorList, String knnString) {
      ensureLeftSideIsAPropertyPath();
      PropertyPath<TypeDescriptor<TypeMetadata>> property = resolveAlias(propertyPath);
      if (property.isEmpty()) {
         throw log.getPredicatesOnEntityAliasNotAllowedException(propertyPath.asStringPath());
      }

      checkIsVector(property);
      Class<?> expectedType = getIndexedPropertyType();

      List<Object> vector = new ArrayList<>(vectorList.size());
      for (String string : vectorList) {
         vector.add(parameterStringValue(string));
      }
      Object knn = parameterStringValue(knnString);

      expressionBuilder.addKnnPredicate(property, expectedType, vector, knn);
   }

   @Override
   public void predicateKNN(String vectorString, String knnString) {
      ensureLeftSideIsAPropertyPath();
      PropertyPath<TypeDescriptor<TypeMetadata>> property = resolveAlias(propertyPath);
      if (property.isEmpty()) {
         throw log.getPredicatesOnEntityAliasNotAllowedException(propertyPath.asStringPath());
      }

      checkIsVector(property);

      ConstantValueExpr.ParamPlaceholder vectorParam = null;
      try {
         vectorParam = (ConstantValueExpr.ParamPlaceholder)
               parameterStringValue(vectorString.substring(1, vectorString.length() - 1));
      } catch (Throwable throwable) {
         throw log.knnVectorParameterNotValid();
      }

      Object knn = parameterStringValue(knnString);
      Class<?> expectedType = getIndexedPropertyType();

      expressionBuilder.addKnnPredicate(property, expectedType, vectorParam, knn);
   }

   @Override
   public void predicateSpatialWithinCircle(String lat, String lon, String radius) {
      ensureLeftSideIsAPropertyPath();
      PropertyPath<TypeDescriptor<TypeMetadata>> property = resolveAlias(propertyPath);
      if (property.isEmpty()) {
         throw log.getPredicatesOnEntityAliasNotAllowedException(propertyPath.asStringPath());
      }
      checkSpatial(property);
      Object latValue = parameterValue(Double.class, lat);
      Object lonValue = parameterValue(Double.class, lon);
      Object radiusValue = parameterValue(Double.class, radius);
      Object unitValue = parameterValue(String.class, (unit == null) ? SpatialWithinCircleExpr.DEFAULT_UNIT : unit);
      if (phase == Phase.WHERE) {
         expressionBuilder.whereBuilder().addSpatialWithinCircle(property, latValue, lonValue, radiusValue, unitValue);
      } else {
         throw new IllegalStateException();
      }
   }

   @Override
   public void predicateSpatialNotWithinCircle(String lat, String lon, String radius) {
      expressionBuilder.whereBuilder().pushNot();
      predicateSpatialWithinCircle(lat, lon, radius);
   }

   @Override
   public void predicateSpatialWithinBox(String tlLat, String tlLon, String brLat, String brLon) {
      ensureLeftSideIsAPropertyPath();
      PropertyPath<TypeDescriptor<TypeMetadata>> property = resolveAlias(propertyPath);
      if (property.isEmpty()) {
         throw log.getPredicatesOnEntityAliasNotAllowedException(propertyPath.asStringPath());
      }
      checkSpatial(property);
      Object tlLatValue = parameterValue(Double.class, tlLat);
      Object tlLonValue = parameterValue(Double.class, tlLon);
      Object brLatValue = parameterValue(Double.class, brLat);
      Object brLonValue = parameterValue(Double.class, brLon);
      if (phase == Phase.WHERE) {
         expressionBuilder.whereBuilder().addSpatialWithinBox(property, tlLatValue, tlLonValue, brLatValue, brLonValue);
      } else {
         throw new IllegalStateException();
      }
   }

   @Override
   public void predicateSpatialNotWithinBox(String tlLat, String tlLon, String brLat, String brLon) {
      expressionBuilder.whereBuilder().pushNot();
      predicateSpatialWithinBox(tlLat, tlLon, brLat, brLon);
   }

   @Override
   public void predicateSpatialWithinPolygon(List<String> vectorList) {
      ensureLeftSideIsAPropertyPath();
      PropertyPath<TypeDescriptor<TypeMetadata>> property = resolveAlias(propertyPath);
      if (property.isEmpty()) {
         throw log.getPredicatesOnEntityAliasNotAllowedException(propertyPath.asStringPath());
      }
      checkSpatial(property);

      List<Object> vector = new ArrayList<>(vectorList.size());
      for (String string : vectorList) {
         vector.add(parameterStringValue(string));
      }
      if (phase == Phase.WHERE) {
         expressionBuilder.whereBuilder().addSpatialWithinPolygon(property, vector);
      } else {
         throw new IllegalStateException();
      }
   }

   @Override
   public void predicateSpatialNotWithinPolygon(List<String> vector) {
      expressionBuilder.whereBuilder().pushNot();
      predicateSpatialWithinPolygon(vector);
   }

   @Override
   public void meters() {
      unit = "m";
   }

   @Override
   public void kilometers() {
      unit = "km";
   }

   @Override
   public void miles() {
      unit = "mi";
   }

   @Override
   public void yards() {
      unit = "yd";
   }

   @Override
   public void nauticalMiles() {
      unit = "nm";
   }

   private void checkAnalyzed(PropertyPath<?> propertyPath, boolean expectAnalyzed) {
      if (!expectAnalyzed) {
         if (fieldIndexingMetadata.isAnalyzed(propertyPath.asArrayPath())) {
            throw log.getQueryOnAnalyzedPropertyNotSupportedException(targetTypeName, propertyPath.asStringPath());
         }
         return;
      }

      if (!fieldIndexingMetadata.isAnalyzed(propertyPath.asArrayPath()) &&
            !fieldIndexingMetadata.isNormalized(propertyPath.asArrayPath())) {
         throw log.getFullTextQueryOnNotAalyzedPropertyNotSupportedException(targetTypeName, propertyPath.asStringPath());
      }
   }

   public void checkIsVector(PropertyPath<?> propertyPath) {
      if (!fieldIndexingMetadata.isVector(propertyPath.asArrayPath())) {
         throw log.knnPredicateOnNotVectorField(targetTypeName, propertyPath.asStringPath());
      }
   }

   private void checkIndexed(PropertyPath<?> propertyPath) {
      if (!fieldIndexingMetadata.isSearchable(propertyPath.asArrayPath())) {
         throw log.getFullTextQueryOnNotIndexedPropertyNotSupportedException(targetTypeName, propertyPath.asStringPath());
      }
   }

   private void checkSpatial(PropertyPath<?> propertyPath) {
      if (!fieldIndexingMetadata.isSpatial(propertyPath.asArrayPath())) {
         throw log.spatialQueryOnNotIndexedPropertyNotSupportedException(targetTypeName, propertyPath.asStringPath());
      }
   }

   @Override
   public void activateFullTextBoost(float boost) {
      expressionBuilder.pushFullTextBoost(boost);
   }

   @Override
   public void deactivateFullTextBoost() {
      expressionBuilder.popFullTextBoost();
   }

   @Override
   public void activateFullTextOccur(Occur occur) {
      expressionBuilder.pushFullTextOccur(occur);
   }

   @Override
   public void deactivateFullTextOccur() {
      expressionBuilder.popFullTextOccur();
   }

   /**
    * Sets a property path representing one property in the SELECT, GROUP BY, WHERE or HAVING clause of a given query.
    *
    * @param propertyPath the property path to set
    */
   @Override
   public void setPropertyPath(PropertyPath<TypeDescriptor<TypeMetadata>> propertyPath) {
      boolean aggregationPropertyPath = false;

      if (function != null) {
         propertyPath = new FunctionPropertyPath<>(propertyPath.getNodes(), function, functionArgs);
      }
      if (aggregationFunction != null) {
         if (propertyPath == null && aggregationFunction != AggregationFunction.COUNT && aggregationFunction != AggregationFunction.COUNT_DISTINCT) {
            throw log.getAggregationCanOnlyBeAppliedToPropertyReferencesException(aggregationFunction.name());
         }
         propertyPath = new AggregationPropertyPath<>(aggregationFunction, propertyPath.getNodes());
         aggregationPropertyPath = true;
      }
      if (phase == Phase.SELECT) {
         if (projections == null) {
            projections = new ArrayList<>(ARRAY_INITIAL_LENGTH);
            projectedTypes = new ArrayList<>(ARRAY_INITIAL_LENGTH);
            projectedNullMarkers = new ArrayList<>(ARRAY_INITIAL_LENGTH);
         }
         PropertyPath<TypeDescriptor<TypeMetadata>> projection;
         Class<?> propertyType;
         Object nullMarker;
         if (propertyPath.getLength() == 1 && propertyPath.isAlias()) {
            if (!aggregationPropertyPath) {
               projection = new CacheValuePropertyPath<>();
            } else {
               projection = new CacheValueAggregationPropertyPath<>();
            }

            propertyType = null;
            nullMarker = null;
         } else {
            if (function == Function.DISTANCE) {
               projection = new FunctionPropertyPath<>(resolveAlias(propertyPath).getNodes(), Function.DISTANCE, functionArgs);
               propertyType = Double.class;
            } else {
               projection = resolveAlias(propertyPath);
               propertyType = propertyHelper.getPrimitivePropertyType(targetEntityMetadata, projection.asArrayPath());
            }
            nullMarker = fieldIndexingMetadata.getNullMarker(projection.asArrayPath());
         }
         projections.add(projection);
         projectedTypes.add(propertyType);
         projectedNullMarkers.add(nullMarker);
      } else {
         this.propertyPath = propertyPath;
      }
   }

   @Override
   public void activateAggregation(AggregationFunction aggregationFunction) {
      if (phase == Phase.WHERE) {
         throw log.getNoAggregationsInWhereClauseException(aggregationFunction.name());
      }
      if (phase == Phase.GROUP_BY) {
         throw log.getNoAggregationsInGroupByClauseException(aggregationFunction.name());
      }
      this.aggregationFunction = aggregationFunction;
      propertyPath = null;
   }

   @Override
   public void activateAsteriskAggregation(AggregationFunction aggregationFunction) {
      activateAggregation(aggregationFunction);
      asteriskCount = true;
   }

   @Override
   public void deactivateAggregation() {
      aggregationFunction = null;
      function = null;
      functionArgs = null;
   }

   @Override
   public void projectVersion() {
      if (phase != Phase.SELECT) {
         return; // do nothing
      }

      if (projections == null) {
         projections = new ArrayList<>(ARRAY_INITIAL_LENGTH);
         projectedTypes = new ArrayList<>(ARRAY_INITIAL_LENGTH);
         projectedNullMarkers = new ArrayList<>(ARRAY_INITIAL_LENGTH);
      }

      projections.add(new VersionPropertyPath<>());
      projectedTypes.add(Object.class); // Usually a core module EntryVersion
      projectedNullMarkers.add(null);
   }

   @Override
   public void projectScore() {
      if (phase != Phase.SELECT) {
         return; // do nothing
      }

      if (projections == null) {
         projections = new ArrayList<>(ARRAY_INITIAL_LENGTH);
         projectedTypes = new ArrayList<>(ARRAY_INITIAL_LENGTH);
         projectedNullMarkers = new ArrayList<>(ARRAY_INITIAL_LENGTH);
      }

      projections.add(new ScorePropertyPath<>());
      projectedTypes.add(Object.class); // Usually a core module EntryVersion
      projectedNullMarkers.add(null);
   }

   /**
    * Add field sort criteria.
    *
    * @param collateName optional collation name
    * @param isAscending sort direction
    */
   @Override
   public void sortSpecification(String collateName, boolean isAscending) {
      // collationName is ignored for now
      PropertyPath<TypeDescriptor<TypeMetadata>> property = resolveAlias(propertyPath);
      checkAnalyzed(property, false);     //todo [anistor] cannot sort on analyzed field?

      if (sortFields == null) {
         sortFields = new ArrayList<>(ARRAY_INITIAL_LENGTH);
      }
      sortFields.add(new IckleParsingResult.SortFieldImpl<>(property, isAscending));
   }

   /**
    * Add 'group by' criteria.
    *
    * @param collateName optional collation name
    */
   @Override
   public void groupingValue(String collateName) {
      // collationName is ignored for now
      if (groupBy == null) {
         groupBy = new ArrayList<>(ARRAY_INITIAL_LENGTH);
      }
      groupBy.add(resolveAlias(propertyPath));

      if (!asteriskCount) {
         return;
      }
      if (projections == null) {
         projections = new ArrayList<>(ARRAY_INITIAL_LENGTH);
         projectedTypes = new ArrayList<>(ARRAY_INITIAL_LENGTH);
         projectedNullMarkers = new ArrayList<>(ARRAY_INITIAL_LENGTH);
      }
      projections.add(new CacheValueAggregationPropertyPath<>());
      projectedTypes.add(null);
      projectedNullMarkers.add(null);
      asteriskCount = false;
   }

   private Object parameterValue(String value) {
      if (value.startsWith(":")) {
         // it's a named parameter
         String paramName = value.substring(1).trim();  //todo [anistor] trim should not be required!
         ConstantValueExpr.ParamPlaceholder namedParam = (ConstantValueExpr.ParamPlaceholder) namedParameters.get(paramName);
         if (namedParam == null) {
            namedParam = new ConstantValueExpr.ParamPlaceholder(paramName);
            namedParameters.put(paramName, namedParam);
         }
         return namedParam;
      } else {
         // it's a literal value given in the query string
         List<String> path = propertyPath.getNodeNamesWithoutAlias();
         // create the complete path in case it's a join
         PropertyPath<TypeDescriptor<TypeMetadata>> fullPath = propertyPath;
         while (fullPath.isAlias()) {
            PropertyPath<TypeDescriptor<TypeMetadata>> resolved = aliasToPropertyPath.get(fullPath.getAlias());
            if (resolved == null) {
               break;
            }
            path.addAll(0, resolved.getNodeNamesWithoutAlias());
            fullPath = resolved;
         }

         return propertyHelper.convertToPropertyType(targetEntityMetadata, path.toArray(new String[path.size()]), value);
      }
   }

   private Object parameterStringValue(String value) {
      if (value.startsWith(":")) {
         // it's a named parameter
         String paramName = value.substring(1).trim();  //todo [anistor] trim should not be required!
         ConstantValueExpr.ParamPlaceholder namedParam = (ConstantValueExpr.ParamPlaceholder) namedParameters.get(paramName);
         if (namedParam == null) {
            namedParam = new ConstantValueExpr.ParamPlaceholder(paramName);
            namedParameters.put(paramName, namedParam);
         }
         return namedParam;
      } else {
         return value;
      }
   }

   public Class<?> getIndexedPropertyType() {
      List<String> path = propertyPath.getNodeNamesWithoutAlias();
      // create the complete path in case it's a join
      PropertyPath<TypeDescriptor<TypeMetadata>> fullPath = propertyPath;
      while (fullPath.isAlias()) {
         PropertyPath<TypeDescriptor<TypeMetadata>> resolved = aliasToPropertyPath.get(fullPath.getAlias());
         if (resolved == null) {
            break;
         }
         path.addAll(0, resolved.getNodeNamesWithoutAlias());
         fullPath = resolved;
      }

      return propertyHelper.getIndexedPropertyType(targetEntityMetadata, path.toArray(new String[path.size()]));
   }

   private Object parameterValue(Class<?> type, String value) {
      if (value.startsWith(":")) {
         // it's a named parameter
         String paramName = value.substring(1).trim();  //todo [anistor] trim should not be required!
         ConstantValueExpr.ParamPlaceholder namedParam = (ConstantValueExpr.ParamPlaceholder) namedParameters.get(paramName);
         if (namedParam == null) {
            namedParam = new ConstantValueExpr.ParamPlaceholder(paramName);
            namedParameters.put(paramName, namedParam);
         }
         return namedParam;
      } else {
         // it's a literal value given in the query string
         return propertyHelper.convertToPropertyType(type, value);
      }
   }

   @Override
   public void deactivateBoolean() {
      expressionBuilder.popBoolean();
   }

   @Override
   public void activateFunction(Function function) {
      this.function = function;
      functionArgs = new ArrayList<>();
      propertyPath = null;
   }

   @Override
   public void deactivateFunction() {
      function = null;
      functionArgs = null;
   }

   @Override
   public void spatialDistance(String lat, String lon) {
      functionArgs.add(Double.parseDouble(lat));
      functionArgs.add(Double.parseDouble(lon));
      if (unit != null) {
         functionArgs.add(unit);
      }
   }

   private PropertyPath<TypeDescriptor<TypeMetadata>> resolveAlias(PropertyPath<TypeDescriptor<TypeMetadata>> path) {
      List<PropertyPath.PropertyReference<TypeDescriptor<TypeMetadata>>> resolved = resolveAliasPath(path);
      if (path instanceof AggregationPropertyPath) {
         return new AggregationPropertyPath<>(((AggregationPropertyPath<TypeMetadata>) path).getAggregationFunction(), resolved);
      }
      if (path instanceof FunctionPropertyPath) {
         return new FunctionPropertyPath<>(resolved, ((FunctionPropertyPath<TypeMetadata>) path).getFunction(),
               ((FunctionPropertyPath<TypeMetadata>) path).getArgs());
      }
      return new PropertyPath<>(resolved);
   }

   private List<PropertyPath.PropertyReference<TypeDescriptor<TypeMetadata>>> resolveAliasPath(PropertyPath<TypeDescriptor<TypeMetadata>> path) {
      if (path.isAlias()) {
         String alias = path.getAlias();
         if (aliasToEntityType.containsKey(alias)) {
            // Alias for entity
            return path.getNodesWithoutAlias();
         } else if (aliasToPropertyPath.containsKey(alias)) {
            // Alias for embedded
            PropertyPath<TypeDescriptor<TypeMetadata>> propertyPath = aliasToPropertyPath.get(alias);
            List<PropertyPath.PropertyReference<TypeDescriptor<TypeMetadata>>> resolvedAlias = resolveAliasPath(propertyPath);
            resolvedAlias.addAll(path.getNodesWithoutAlias());
            return resolvedAlias;
         } else {
            // Alias not found
            throw log.getUnknownAliasException(alias);
         }
      }

      // It does not start with an alias
      return path.getNodesWithoutAlias();
   }

   @Override
   public void registerJoinAlias(Tree alias, PropertyPath<TypeDescriptor<TypeMetadata>> path) {
      if (!aliasToPropertyPath.containsKey(alias.getText())) {
         aliasToPropertyPath.put(alias.getText(), path);
      }
   }

   private void ensureLeftSideIsAPropertyPath() {
      if (propertyPath == null) {
         throw log.getLeftSideMustBeAPropertyPath();
      }
   }

   public IckleParsingResult<TypeMetadata> getResult() {
      return new IckleParsingResult<>(
            queryString,
            statementType,
            Collections.unmodifiableSet(new HashSet<>(namedParameters.keySet())),
            expressionBuilder.whereBuilder().build(),
            expressionBuilder.havingBuilder().build(),
            expressionBuilder.filteringBuilder().build(),
            targetTypeName,
            targetEntityMetadata,
            projections == null ? null : projections.toArray(new PropertyPath[projections.size()]),
            projectedTypes == null ? null : projectedTypes.toArray(new Class<?>[projectedTypes.size()]),
            projectedNullMarkers == null ? null : projectedNullMarkers.toArray(new Object[projectedNullMarkers.size()]),
            groupBy == null ? null : groupBy.toArray(new PropertyPath[groupBy.size()]),
            sortFields == null ? null : sortFields.toArray(new SortField[sortFields.size()]));
   }
}
