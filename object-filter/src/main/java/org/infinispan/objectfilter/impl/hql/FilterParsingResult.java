package org.infinispan.objectfilter.impl.hql;

import org.infinispan.objectfilter.PropertyPath;
import org.infinispan.objectfilter.SortField;
import org.infinispan.objectfilter.impl.syntax.BooleanExpr;

import java.util.Arrays;
import java.util.Set;

/**
 * @param <TypeMetadata> is either {@link java.lang.Class} or {@link org.infinispan.protostream.descriptors.Descriptor}
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class FilterParsingResult<TypeMetadata> {

   static final class SortFieldImpl implements SortField {

      public final PropertyPath path;

      public final boolean isAscending;

      SortFieldImpl(PropertyPath path, boolean isAscending) {
         this.path = path;
         this.isAscending = isAscending;
      }

      public PropertyPath getPath() {
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

   private final String jpaQuery;
   private final Set<String> parameterNames;
   private final BooleanExpr whereClause;
   private final BooleanExpr havingClause;
   private final String targetEntityName;
   private final TypeMetadata targetEntityMetadata;
   private final PropertyPath[] projectedPaths;
   private final Class<?>[] projectedTypes;
   private final PropertyPath[] groupBy;
   private final SortField[] sortFields;

   //todo [anistor] make package local
   public FilterParsingResult(String jpaQuery,
                              Set<String> parameterNames,
                              BooleanExpr whereClause,
                              BooleanExpr havingClause,
                              String targetEntityName, TypeMetadata targetEntityMetadata,
                              PropertyPath[] projectedPaths, Class<?>[] projectedTypes,
                              PropertyPath[] groupBy,
                              SortField[] sortFields) {
      this.jpaQuery = jpaQuery;
      this.parameterNames = parameterNames;
      this.whereClause = whereClause;
      this.havingClause = havingClause;
      this.targetEntityName = targetEntityName;
      this.targetEntityMetadata = targetEntityMetadata;
      if (projectedPaths != null && (projectedTypes == null || projectedTypes.length != projectedPaths.length) || projectedPaths == null && projectedTypes != null) {
         throw new IllegalArgumentException("projectedPaths and projectedTypes sizes must match");
      }
      this.projectedPaths = projectedPaths;
      this.projectedTypes = projectedTypes;
      this.groupBy = groupBy;
      this.sortFields = sortFields;
   }

   public String getJpaQuery() {
      return jpaQuery;
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

   /**
    * Returns the original entity name as given in the query
    *
    * @return the entity name of the query
    */
   public String getTargetEntityName() {
      return targetEntityName;
   }

   /**
    * Returns the entity type of the parsed query as derived from the queried entity name via the configured {@link
    * org.hibernate.hql.ast.spi.EntityNamesResolver}.
    *
    * @return the entity type of the parsed query
    */
   public TypeMetadata getTargetEntityMetadata() {
      return targetEntityMetadata;
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
         projections[i] = projectedPaths[i].asStringPath();
      }
      return projections;
   }

   public PropertyPath[] getProjectedPaths() {
      return projectedPaths;
   }

   public Class<?>[] getProjectedTypes() {
      return projectedTypes;
   }

   public boolean hasGroupingOrAggregations() {
      if (groupBy != null || havingClause != null) {
         return true;
      }
      if (projectedPaths != null) {
         for (PropertyPath p : projectedPaths) {
            if (p.getAggregationType() != null) {
               return true;
            }
         }
      }
      if (sortFields != null) {
         for (SortField s : sortFields) {
            if (s.getPath().getAggregationType() != null) {
               return true;
            }
         }
      }
      return false;
   }

   public PropertyPath[] getGroupBy() {
      return groupBy;
   }

   public SortField[] getSortFields() {
      return sortFields;
   }

   @Override
   public String toString() {
      return "FilterParsingResult [" +
            " jpaQuery=" + jpaQuery
            + ", targetEntityName=" + targetEntityName
            + ", parameterNames=" + parameterNames
            + ", whereClause=" + whereClause
            + ", havingClause=" + havingClause
            + ", projectedPaths=" + Arrays.toString(projectedPaths)
            + ", projectedTypes=" + Arrays.toString(projectedTypes)
            + ", groupBy=" + Arrays.toString(groupBy)
            + ", sortFields=" + Arrays.toString(sortFields)
            + "]";
   }
}
