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
import org.infinispan.objectfilter.impl.ql.JoinType;
import org.infinispan.objectfilter.impl.ql.PropertyPath;
import org.infinispan.objectfilter.impl.ql.QueryRendererDelegate;
import org.infinispan.objectfilter.impl.syntax.ComparisonExpr;
import org.infinispan.objectfilter.impl.syntax.ConstantValueExpr;
import org.infinispan.objectfilter.impl.syntax.IndexedFieldProvider;
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

   private enum Phase {
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
   private Phase phase;

   private String targetTypeName;

   private TypeMetadata targetEntityMetadata;

   private IndexedFieldProvider.FieldIndexingMetadata fieldIndexingMetadata;

   private PropertyPath<TypeDescriptor<TypeMetadata>> propertyPath;

   private AggregationFunction aggregationFunction;

   private final ExpressionBuilder<TypeMetadata> whereBuilder;

   private final ExpressionBuilder<TypeMetadata> havingBuilder;

   /**
    * Persister space: keep track of aliases and entity names.
    */
   private final Map<String, String> aliasToEntityType = new HashMap<>();

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

   QueryRendererDelegateImpl(String queryString, ObjectPropertyHelper<TypeMetadata> propertyHelper) {
      this.queryString = queryString;
      this.propertyHelper = propertyHelper;
      this.whereBuilder = new ExpressionBuilder<>(propertyHelper);
      this.havingBuilder = new ExpressionBuilder<>(propertyHelper);
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
      whereBuilder.setEntityType(targetEntityMetadata);
      havingBuilder.setEntityType(targetEntityMetadata);
   }

   public void registerEmbeddedAlias(String alias, PropertyPath<TypeDescriptor<TypeMetadata>> propertyPath) {
      PropertyPath<TypeDescriptor<TypeMetadata>> previous = aliasToPropertyPath.put(alias, propertyPath);
      if (previous != null) {
         throw new UnsupportedOperationException("Alias reuse currently not supported: alias " + alias + " already assigned to type " + previous);
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
   public void deactivateStrategy() {
      phase = null;
      alias = null;
      propertyPath = null;
      aggregationFunction = null;
   }

   @Override
   public void activateOR() {
      if (phase == Phase.WHERE) {
         whereBuilder.pushOr();
      } else if (phase == Phase.HAVING) {
         havingBuilder.pushOr();
      } else {
         throw new IllegalStateException();
      }
   }

   @Override
   public void activateAND() {
      if (phase == Phase.WHERE) {
         whereBuilder.pushAnd();
      } else if (phase == Phase.HAVING) {
         havingBuilder.pushAnd();
      } else {
         throw new IllegalStateException();
      }
   }

   @Override
   public void activateNOT() {
      if (phase == Phase.WHERE) {
         whereBuilder.pushNot();
      } else if (phase == Phase.HAVING) {
         havingBuilder.pushNot();
      } else {
         throw new IllegalStateException();
      }
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
      Object comparisonValue = parameterValue(value);
      checkAnalyzed(property, false);
      if (phase == Phase.WHERE) {
         whereBuilder.addComparison(property, comparisonType, comparisonValue);
      } else if (phase == Phase.HAVING) {
         havingBuilder.addComparison(property, comparisonType, comparisonValue);
      } else {
         throw new IllegalStateException();
      }
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
      if (phase == Phase.WHERE) {
         whereBuilder.addIn(property, values);
      } else if (phase == Phase.HAVING) {
         havingBuilder.addIn(property, values);
      } else {
         throw new IllegalStateException();
      }
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
      if (phase == Phase.WHERE) {
         whereBuilder.addRange(property, lowerComparisonValue, upperComparisonValue);
      } else if (phase == Phase.HAVING) {
         havingBuilder.addRange(property, lowerComparisonValue, upperComparisonValue);
      } else {
         throw new IllegalStateException();
      }
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
      if (phase == Phase.WHERE) {
         whereBuilder.addLike(property, pattern, escapeCharacter);
      } else if (phase == Phase.HAVING) {
         havingBuilder.addLike(property, pattern, escapeCharacter);
      } else {
         throw new IllegalStateException();
      }
   }

   @Override
   public void predicateIsNull() {
      ensureLeftSideIsAPropertyPath();
      PropertyPath<TypeDescriptor<TypeMetadata>> property = resolveAlias(propertyPath);
      if (property.isEmpty()) {
         throw log.getPredicatesOnEntityAliasNotAllowedException(propertyPath.asStringPath());
      }
      checkAnalyzed(property, false);
      if (phase == Phase.WHERE) {
         whereBuilder.addIsNull(property);
      } else if (phase == Phase.HAVING) {
         havingBuilder.addIsNull(property);
      } else {
         throw new IllegalStateException();
      }
   }

   @Override
   public void predicateConstantBoolean(boolean booleanConstant) {
      if (phase == Phase.WHERE) {
         whereBuilder.addConstantBoolean(booleanConstant);
      } else if (phase == Phase.HAVING) {
         havingBuilder.addConstantBoolean(booleanConstant);
      } else {
         throw new IllegalStateException();
      }
   }

   @Override
   public void predicateFullTextTerm(String term, String fuzzyFlop) {
      ensureLeftSideIsAPropertyPath();
      PropertyPath<TypeDescriptor<TypeMetadata>> property = resolveAlias(propertyPath);
      if (property.isEmpty()) {
         throw log.getPredicatesOnEntityAliasNotAllowedException(propertyPath.asStringPath());
      }
      if (phase == Phase.WHERE) {
         checkAnalyzed(property, true);
         Object comparisonObject = parameterValue(term);
         whereBuilder.addFullTextTerm(property, comparisonObject.toString(), fuzzyFlop == null ? null : (fuzzyFlop.equals("~") ? 2 : Integer.parseInt(fuzzyFlop)));
      } else if (phase == Phase.HAVING) {
         throw log.getFullTextQueriesNotAllowedInHavingClauseException();
      } else {
         throw new IllegalStateException();
      }
   }

   @Override
   public void predicateFullTextRegexp(String term) {
      ensureLeftSideIsAPropertyPath();
      PropertyPath<TypeDescriptor<TypeMetadata>> property = resolveAlias(propertyPath);
      if (property.isEmpty()) {
         throw log.getPredicatesOnEntityAliasNotAllowedException(propertyPath.asStringPath());
      }
      if (phase == Phase.WHERE) {
         checkAnalyzed(property, true);
         whereBuilder.addFullTextRegexp(property, term);
      } else if (phase == Phase.HAVING) {
         throw log.getFullTextQueriesNotAllowedInHavingClauseException();
      } else {
         throw new IllegalStateException();
      }
   }

   @Override
   public void predicateFullTextRange(boolean includeLower, String lower, String upper, boolean includeUpper) {
      ensureLeftSideIsAPropertyPath();
      if (phase == Phase.WHERE) {
         PropertyPath<TypeDescriptor<TypeMetadata>> property = resolveAlias(propertyPath);
         if (property.isEmpty()) {
            throw log.getPredicatesOnEntityAliasNotAllowedException(propertyPath.asStringPath());
         }
         Object from = lower != null ? parameterValue(lower) : null;
         Object to = upper != null ? parameterValue(upper) : null;
         checkIndexed(property);
         whereBuilder.addFullTextRange(property, includeLower, from, to, includeUpper);
      } else if (phase == Phase.HAVING) {
         throw log.getFullTextQueriesNotAllowedInHavingClauseException();
      } else {
         throw new IllegalStateException();
      }
   }

   private void checkAnalyzed(PropertyPath<?> propertyPath, boolean expectAnalyzed) {
      if (fieldIndexingMetadata.isAnalyzed(propertyPath.asArrayPath())) {
         if (!expectAnalyzed) {
            throw log.getQueryOnAnalyzedPropertyNotSupportedException(targetTypeName, propertyPath.asStringPath());
         }
      } else {
         if (expectAnalyzed) {
            throw log.getFullTextQueryOnNotAalyzedPropertyNotSupportedException(targetTypeName, propertyPath.asStringPath());
         }
      }
   }

   private void checkIndexed(PropertyPath<?> propertyPath) {
      if (!fieldIndexingMetadata.isIndexed(propertyPath.asArrayPath())) {
         throw log.getFullTextQueryOnNotIndexedPropertyNotSupportedException(targetTypeName, propertyPath.asStringPath());
      }
   }

   @Override
   public void activateFullTextBoost(float boost) {
      if (phase == Phase.WHERE) {
         whereBuilder.pushFullTextBoost(boost);
      } else if (phase == Phase.HAVING) {
         throw log.getFullTextQueriesNotAllowedInHavingClauseException();
      } else {
         throw new IllegalStateException();
      }
   }

   @Override
   public void deactivateFullTextBoost() {
      if (phase == Phase.WHERE) {
         whereBuilder.pop();
      } else if (phase == Phase.HAVING) {
         throw log.getFullTextQueriesNotAllowedInHavingClauseException();
      } else {
         throw new IllegalStateException();
      }
   }

   @Override
   public void activateFullTextOccur(Occur occur) {
      if (phase == Phase.WHERE) {
         whereBuilder.pushFullTextOccur(occur);
      } else if (phase == Phase.HAVING) {
         throw log.getFullTextQueriesNotAllowedInHavingClauseException();
      } else {
         throw new IllegalStateException();
      }
   }

   @Override
   public void deactivateFullTextOccur() {
      if (phase == Phase.WHERE) {
         whereBuilder.pop();
      } else if (phase == Phase.HAVING) {
         throw log.getFullTextQueriesNotAllowedInHavingClauseException();
      } else {
         throw new IllegalStateException();
      }
   }

   /**
    * Sets a property path representing one property in the SELECT, GROUP BY, WHERE or HAVING clause of a given query.
    *
    * @param propertyPath the property path to set
    */
   @Override
   public void setPropertyPath(PropertyPath<TypeDescriptor<TypeMetadata>> propertyPath) {
      if (aggregationFunction != null) {
         if (propertyPath == null && aggregationFunction != AggregationFunction.COUNT && aggregationFunction != AggregationFunction.COUNT_DISTINCT) {
            throw log.getAggregationCanOnlyBeAppliedToPropertyReferencesException(aggregationFunction.name());
         }
         propertyPath = new AggregationPropertyPath<>(aggregationFunction, propertyPath.getNodes());
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
            projection = new PropertyPath<>(Collections.singletonList(new PropertyPath.PropertyReference<>("__HSearch_This", null, true))); //todo [anistor] this is a leftover from hsearch ????   this represents the entity itself. see org.hibernate.search.ProjectionConstants
            propertyType = null;
            nullMarker = null;
         } else {
            projection = resolveAlias(propertyPath);
            propertyType = propertyHelper.getPrimitivePropertyType(targetEntityMetadata, projection.asArrayPath());
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
   public void deactivateAggregation() {
      aggregationFunction = null;
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
            PropertyPath<TypeDescriptor<TypeMetadata>> resolved = aliasToPropertyPath.get(fullPath.getFirst().getPropertyName());
            if (resolved == null) {
               break;
            }
            path.addAll(0, resolved.getNodeNamesWithoutAlias());
            fullPath = resolved;
         }

         return propertyHelper.convertToPropertyType(targetEntityMetadata, path.toArray(new String[path.size()]), value);
      }
   }

   @Override
   public void deactivateBoolean() {
      if (phase == Phase.WHERE) {
         whereBuilder.pop();
      } else if (phase == Phase.HAVING) {
         havingBuilder.pop();
      } else {
         throw new IllegalStateException();
      }
   }

   private PropertyPath<TypeDescriptor<TypeMetadata>> resolveAlias(PropertyPath<TypeDescriptor<TypeMetadata>> path) {
      List<PropertyPath.PropertyReference<TypeDescriptor<TypeMetadata>>> resolved = resolveAliasPath(path);
      return path instanceof AggregationPropertyPath ?
            new AggregationPropertyPath<>(((AggregationPropertyPath) path).getAggregationFunction(), resolved) : new PropertyPath<>(resolved);
   }

   private List<PropertyPath.PropertyReference<TypeDescriptor<TypeMetadata>>> resolveAliasPath(PropertyPath<TypeDescriptor<TypeMetadata>> path) {
      if (path.isAlias()) {
         String alias = path.getFirst().getPropertyName();
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
            Collections.unmodifiableSet(new HashSet<>(namedParameters.keySet())),
            whereBuilder.build(),
            havingBuilder.build(),
            targetTypeName,
            targetEntityMetadata,
            projections == null ? null : projections.toArray(new PropertyPath[projections.size()]),
            projectedTypes == null ? null : projectedTypes.toArray(new Class<?>[projectedTypes.size()]),
            projectedNullMarkers == null ? null : projectedNullMarkers.toArray(new Object[projectedNullMarkers.size()]),
            groupBy == null ? null : groupBy.toArray(new PropertyPath[groupBy.size()]),
            sortFields == null ? null : sortFields.toArray(new SortField[sortFields.size()]));
   }
}
