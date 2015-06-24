package org.infinispan.objectfilter.impl.hql;

import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.infinispan.objectfilter.PropertyPath;
import org.infinispan.objectfilter.SortField;
import org.infinispan.objectfilter.impl.syntax.BooleanExpr;

import java.util.ArrayList;
import java.util.Collections;
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

   private final BooleanExpr whereFilter;
   private final BooleanExpr havingFilter;
   private final String targetEntityName;
   private final TypeMetadata targetEntityMetadata;
   private final List<PropertyPath> projectedPaths;
   private final List<PropertyPath> groupBy;
   private final List<SortField> sortFields;

   FilterParsingResult(BooleanExpr whereFilter, BooleanExpr havingFilter, String targetEntityName, TypeMetadata targetEntityMetadata,
                       List<PropertyPath> projectedPaths, List<PropertyPath> groupBy, List<SortField> sortFields) {
      this.whereFilter = whereFilter;
      this.havingFilter = havingFilter;
      this.targetEntityName = targetEntityName;
      this.targetEntityMetadata = targetEntityMetadata;
      this.projectedPaths = projectedPaths != null ? projectedPaths : Collections.<PropertyPath>emptyList();
      this.groupBy = groupBy != null ? groupBy : Collections.<PropertyPath>emptyList();
      this.sortFields = sortFields != null ? sortFields : Collections.<SortField>emptyList();
   }

   /**
    * Returns the filter created while walking the parse tree.
    *
    * @return the filter created while walking the parse tree
    */
   public BooleanExpr getWhereClause() {
      return whereFilter;
   }

   public BooleanExpr getHavingClause() {
      return havingFilter;
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
    * EntityNamesResolver}.
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
    * @return a list with the projections of the parsed query; an empty list will be returned if no the query has no
    * projections
    */
   public List<String> getProjections() {
      List<String> projections = new ArrayList<>(projectedPaths.size());
      for (PropertyPath p : projectedPaths) {
         projections.add(p.asStringPath());
      }
      return projections;
   }

   public List<PropertyPath> getProjectedPaths() {
      return projectedPaths;
   }

   public boolean hasGroupingOrAggregations() {
      if (havingFilter != null || !groupBy.isEmpty()) {
         return true;
      }
      for (PropertyPath p : projectedPaths) {
         if (p.getAggregationType() != null) {
            return true;
         }
      }
      for (SortField s : sortFields) {
         if (s.getPath().getAggregationType() != null) {
            return true;
         }
      }
      return false;
   }

   public List<PropertyPath> getGroupBy() {
      return groupBy;
   }

   public List<SortField> getSortFields() {
      return sortFields;
   }

   @Override
   public String toString() {
      return "FilterParsingResult [filter=" + whereFilter + ", havingFilter=" + havingFilter + ", targetEntityName=" + targetEntityName
            + ", projectedPaths=" + projectedPaths + ", sortFields=" + sortFields + "]";
   }
}
