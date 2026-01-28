package org.infinispan.query.objectfilter.impl.syntax.parser;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.Proto;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.query.objectfilter.SortField;
import org.infinispan.query.objectfilter.impl.ql.PropertyPath;
import org.infinispan.query.objectfilter.impl.syntax.BooleanExpr;
import org.infinispan.query.objectfilter.impl.syntax.parser.projection.ScorePropertyPath;

/**
 * @param <TypeMetadata> is either {@link Class} or {@link org.infinispan.protostream.descriptors.Descriptor}
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class IckleParsingResult<TypeMetadata> {

   @Proto
   @ProtoTypeId(ProtoStreamTypeIds.ICKLE_PARSING_RESULT_STATEMENT_TYPE)
   public enum StatementType {
      SELECT,
      DELETE;

      private static final StatementType[] CACHED_VALUES = StatementType.values();

      public static StatementType valueOf(int index) {
         return CACHED_VALUES[index];
      }
   }

   static final class SortFieldImpl<TypeMetadata> implements SortField {

      public final PropertyPath<TypeDescriptor<TypeMetadata>> path;

      public final boolean isAscending;

      SortFieldImpl(PropertyPath<TypeDescriptor<TypeMetadata>> path, boolean isAscending) {
         this.path = path;
         this.isAscending = isAscending;
      }

      public PropertyPath<TypeDescriptor<TypeMetadata>> getPath() {
         return path;
      }

      public boolean isAscending() {
         return isAscending;
      }

      @Override
      public String toString() {
         return "SortField(" + path + ", " + (isAscending ? "ASC" : "DESC") + ')';
      }
   }

   private final String queryString;
   private final StatementType statementType;
   private final Set<String> parameterNames;
   private final BooleanExpr whereClause;
   private final BooleanExpr havingClause;
   private final BooleanExpr filteringClause;
   private final String targetEntityName;
   private final TypeMetadata targetEntityMetadata;
   private final PropertyPath<?>[] projectedPaths;
   private final Class<?>[] projectedTypes;
   private final Object[] projectedNullMarkers;
   private final PropertyPath<?>[] groupBy;
   private final SortField[] sortFields;

   public static class Builder<TypeMetadata> {
      private String queryString;
      private StatementType statementType;
      private Set<String> parameterNames = new HashSet<>();
      private BooleanExpr whereClause;
      private BooleanExpr havingClause;
      private BooleanExpr filteringClause;
      private String targetEntityName;
      private TypeMetadata targetEntityMetadata;
      private PropertyPath<?>[] projectedPaths;
      private Class<?>[] projectedTypes;
      private Object[] projectedNullMarkers;
      private PropertyPath<?>[] groupBy;
      private SortField[] sortFields;

      public Builder<TypeMetadata> setQueryString(String queryString) {
         this.queryString = queryString;
         return this;
      }

      public Builder<TypeMetadata> setStatementType(StatementType statementType) {
         this.statementType = statementType;
         return this;
      }

      public Builder<TypeMetadata> setParameterNames(Set<String> parameterNames) {
         this.parameterNames.addAll(parameterNames);
         return this;
      }

      public Builder<TypeMetadata> setWhereClause(BooleanExpr whereClause) {
         this.whereClause = whereClause;
         return this;
      }

      public Builder<TypeMetadata> setHavingClause(BooleanExpr havingClause) {
         this.havingClause = havingClause;
         return this;
      }

      public Builder<TypeMetadata> setFilteringClause(BooleanExpr filteringClause) {
         this.filteringClause = filteringClause;
         return this;
      }

      public Builder<TypeMetadata> setTargetEntityName(String targetEntityName) {
         this.targetEntityName = targetEntityName;
         return this;
      }

      public Builder<TypeMetadata> setTargetEntityMetadata(TypeMetadata targetEntityMetadata) {
         this.targetEntityMetadata = targetEntityMetadata;
         return this;
      }

      public Builder<TypeMetadata> setProjectedPaths(PropertyPath<?>[] projectedPaths) {
         this.projectedPaths = projectedPaths;
         return this;
      }

      public Builder<TypeMetadata> setProjectedTypes(Class<?>[] projectedTypes) {
         this.projectedTypes = projectedTypes;
         return this;
      }

      public Builder<TypeMetadata> setProjectedNullMarkers(Object[] projectedNullMarkers) {
         this.projectedNullMarkers = projectedNullMarkers;
         return this;
      }

      public Builder<TypeMetadata> setGroupBy(PropertyPath<?>[] groupBy) {
         this.groupBy = groupBy;
         return this;
      }

      public Builder<TypeMetadata> setSortFields(SortField[] sortFields) {
         this.sortFields = sortFields;
         return this;
      }

      public IckleParsingResult<TypeMetadata> build() {
         return new IckleParsingResult<>(
               queryString,
               statementType,
               parameterNames,
               whereClause,
               havingClause,
               filteringClause,
               targetEntityName,
               targetEntityMetadata,
               projectedPaths,
               projectedTypes,
               projectedNullMarkers,
               groupBy,
               sortFields
         );
      }
   }

   public IckleParsingResult(String queryString,
                             StatementType statementType,
                             Set<String> parameterNames,
                             BooleanExpr whereClause, BooleanExpr havingClause, BooleanExpr filteringClause,
                             String targetEntityName, TypeMetadata targetEntityMetadata,
                             PropertyPath<?>[] projectedPaths, Class<?>[] projectedTypes, Object[] projectedNullMarkers,
                             PropertyPath<?>[] groupBy,
                             SortField[] sortFields) {
      this.queryString = queryString;
      this.statementType = statementType;
      this.parameterNames = parameterNames;
      this.whereClause = whereClause;
      this.havingClause = havingClause;
      this.filteringClause = filteringClause;
      this.targetEntityName = targetEntityName;
      this.targetEntityMetadata = targetEntityMetadata;
      if (projectedPaths != null && (projectedTypes == null || projectedTypes.length != projectedPaths.length) || projectedPaths == null && projectedTypes != null) {
         throw new IllegalArgumentException("projectedPaths and projectedTypes sizes must match");
      }
      this.projectedPaths = projectedPaths;
      this.projectedTypes = projectedTypes;
      this.projectedNullMarkers = projectedNullMarkers;
      this.groupBy = groupBy;
      this.sortFields = sortFields;
   }

   public String getQueryString() {
      return queryString;
   }

   public StatementType getStatementType() {
      return statementType;
   }

   public Set<String> getParameterNames() {
      return parameterNames;
   }

   /**
    * Returns the filter created while walking the parse tree.
    *
    * @return the filter created while walking the parse tree
    */
   public BooleanExpr getWhereClause() {
      return whereClause;
   }

   public BooleanExpr getHavingClause() {
      return havingClause;
   }

   public BooleanExpr getFilteringClause() {
      return filteringClause;
   }

   /**
    * Returns the original entity name as given in the query
    *
    * @return the entity name of the query
    */
   public String getTargetEntityName() {
      return targetEntityName;
   }

   /**
    * Returns the entity type of the parsed query.
    *
    * @return the entity type of the parsed query
    */
   public TypeMetadata getTargetEntityMetadata() {
      return targetEntityMetadata;
   }

   public boolean hasScoreProjection() {
      if (projectedPaths == null) {
         return false;
      }

      return Arrays.stream(projectedPaths).sequential().anyMatch(propertyPath -> ScorePropertyPath.SCORE_PROPERTY_NAME.equals(propertyPath.asStringPath()));
   }

   /**
    * Returns the projections of the parsed query, represented as dot paths in case of references to fields of embedded
    * entities, e.g. {@code ["foo", "bar.qaz"]}.
    *
    * @return an array with the projections of the parsed query or null if the query has no projections
    */
   public String[] getProjections() {
      if (projectedPaths == null) {
         return null;
      }
      String[] projections = new String[projectedPaths.length];
      for (int i = 0; i < projectedPaths.length; i++) {
         projections[i] = projectedPaths[i].toString();
      }
      return projections;
   }

   public PropertyPath<?>[] getProjectedPaths() {
      return projectedPaths;
   }

   public Class<?>[] getProjectedTypes() {
      return projectedTypes;
   }

   public Object[] getProjectedNullMarkers() {
      return projectedNullMarkers;
   }

   public boolean hasGroupingOrAggregations() {
      if (groupBy != null || havingClause != null) {
         return true;
      }
      if (projectedPaths != null) {
         for (PropertyPath<?> p : projectedPaths) {
            if (p instanceof AggregationPropertyPath) {
               return true;
            }
         }
      }
      if (sortFields != null) {
         for (SortField s : sortFields) {
            if (s.getPath() instanceof AggregationPropertyPath) {
               return true;
            }
         }
      }
      return false;
   }

   public PropertyPath<?>[] getGroupBy() {
      return groupBy;
   }

   public SortField[] getSortFields() {
      return sortFields;
   }

   @Override
   public String toString() {
      return "IckleParsingResult{" +
            "queryString='" + queryString + '\'' +
            ", statementType=" + statementType +
            ", parameterNames=" + parameterNames +
            ", whereClause=" + whereClause +
            ", havingClause=" + havingClause +
            ", filteringClause=" + filteringClause +
            ", targetEntityName='" + targetEntityName + '\'' +
            ", targetEntityMetadata=" + targetEntityMetadata +
            ", projectedPaths=" + Arrays.toString(projectedPaths) +
            ", projectedTypes=" + Arrays.toString(projectedTypes) +
            ", projectedNullMarkers=" + Arrays.toString(projectedNullMarkers) +
            ", groupBy=" + Arrays.toString(groupBy) +
            ", sortFields=" + Arrays.toString(sortFields) +
            '}';
   }
}
