package org.infinispan.objectfilter.impl.hql;

import org.hibernate.hql.ast.spi.SingleEntityHavingQueryBuilder;
import org.hibernate.hql.ast.spi.SingleEntityQueryBuilder;
import org.infinispan.objectfilter.PropertyPath;
import org.infinispan.objectfilter.SortField;
import org.infinispan.objectfilter.impl.syntax.BooleanExpr;

import java.util.Arrays;
import java.util.List;

/**
 * @param <TypeMetadata> is either {@link java.lang.Class} or {@link org.infinispan.protostream.descriptors.Descriptor}
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class FilterParsingResult<TypeMetadata> {

   public static final class SortFieldImpl implements SortField {

      public final PropertyPath path;

      public final boolean isAscending;

      public SortFieldImpl(PropertyPath path, boolean isAscending) {
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

   private final SingleEntityQueryBuilder<BooleanExpr> whereBuilder;
   private final SingleEntityHavingQueryBuilder<BooleanExpr> havingBuilder;
   private final String targetEntityName;
   private final TypeMetadata targetEntityMetadata;
   private final PropertyPath[] projectedPaths;
   private final Class<?>[] projectedTypes;
   private final PropertyPath[] groupBy;
   private final SortField[] sortFields;

   FilterParsingResult(SingleEntityQueryBuilder<BooleanExpr> whereBuilder,
                       SingleEntityHavingQueryBuilder<BooleanExpr> havingBuilder,
                       String targetEntityName, TypeMetadata targetEntityMetadata,
                       List<PropertyPath> projectedPaths,
                       List<Class<?>> projectedTypes,
                       List<PropertyPath> groupBy,
                       List<SortField> sortFields) {
      this.whereBuilder = whereBuilder;
      this.havingBuilder = havingBuilder;
      this.targetEntityName = targetEntityName;
      this.targetEntityMetadata = targetEntityMetadata;
      if (projectedPaths != null && (projectedTypes == null || projectedTypes.size() != projectedPaths.size()) || projectedPaths == null && projectedTypes != null) {
         throw new IllegalArgumentException("projectedPaths and projectedTypes sizes must match");
      }
      this.projectedPaths = projectedPaths == null ? null : projectedPaths.toArray(new PropertyPath[projectedPaths.size()]);
      this.projectedTypes = projectedTypes == null ? null : projectedTypes.toArray(new Class<?>[projectedTypes.size()]);
      this.groupBy = groupBy == null ? null : groupBy.toArray(new PropertyPath[groupBy.size()]);
      this.sortFields = sortFields == null ? null : sortFields.toArray(new SortField[sortFields.size()]);
   }

   /**
    * Returns the filter created while walking the parse tree.
    *
    * @return the filter created while walking the parse tree
    */
   public BooleanExpr getWhereClause() {
      return whereBuilder.build();
   }

   public BooleanExpr getHavingClause() {
      return havingBuilder.build();
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
      //todo [anistor] havingBuilder.build() builds the query prematurely and is inefficient
      if (groupBy != null || havingBuilder.build() != null) {
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
            "targetEntityName=" + targetEntityName
            + ", whereClause=" + getWhereClause()
            + ", havingClause=" + getHavingClause()
            + ", projectedPaths=" + Arrays.toString(projectedPaths)
            + ", projectedTypes=" + Arrays.toString(projectedTypes)
            + ", groupBy=" + Arrays.toString(groupBy)
            + ", sortFields=" + Arrays.toString(sortFields)
            + "]";
   }
}
