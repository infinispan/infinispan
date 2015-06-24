package org.infinispan.objectfilter.impl.hql;

import org.antlr.runtime.tree.Tree;
import org.hibernate.hql.ast.origin.hql.resolve.path.AggregationPropertyPath;
import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.hibernate.hql.ast.spi.SingleEntityHavingQueryBuilder;
import org.hibernate.hql.ast.spi.SingleEntityQueryBuilder;
import org.hibernate.hql.ast.spi.SingleEntityQueryRendererDelegate;
import org.infinispan.objectfilter.PropertyPath;
import org.infinispan.objectfilter.SortField;
import org.infinispan.objectfilter.impl.syntax.BooleanExpr;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class FilterRendererDelegate<TypeMetadata> extends SingleEntityQueryRendererDelegate<BooleanExpr, FilterParsingResult> {

   private final ObjectPropertyHelper<TypeMetadata> propertyHelper;

   private final SingleEntityHavingQueryBuilder<BooleanExpr> havingBuilder;

   private TypeMetadata targetEntityMetadata;

   private List<PropertyPath> groupBy;

   private List<SortField> sortFields;

   private final List<PropertyPath> projections = new ArrayList<PropertyPath>();

   public FilterRendererDelegate(EntityNamesResolver entityNamesResolver, ObjectPropertyHelper<TypeMetadata> propertyHelper,
                                 SingleEntityQueryBuilder<BooleanExpr> builder, SingleEntityHavingQueryBuilder<BooleanExpr> havingBuilder, Map<String, Object> namedParameters) {
      super(propertyHelper, entityNamesResolver, builder, namedParameters);
      this.propertyHelper = propertyHelper;
      this.havingBuilder = havingBuilder;
   }

   @Override
   protected SingleEntityHavingQueryBuilder<BooleanExpr> getHavingBuilder() {
      return havingBuilder;
   }

   @Override
   protected void addSortField(org.hibernate.hql.ast.origin.hql.resolve.path.PropertyPath propertyPath, String collateName, boolean isAscending) {
      // collationName is ignored
      if (sortFields == null) {
         sortFields = new ArrayList<SortField>(5);
      }
      sortFields.add(new FilterParsingResult.SortFieldImpl(makePropertyPath(propertyPath), isAscending));
   }

   private PropertyPath makePropertyPath(org.hibernate.hql.ast.origin.hql.resolve.path.PropertyPath propertyPath) {
      AggregationPropertyPath.Type aggregationType = propertyPath instanceof AggregationPropertyPath ? ((AggregationPropertyPath) propertyPath).getType() : null;
      return new PropertyPath(PropertyPath.AggregationType.from(aggregationType), propertyPath.getNodeNamesWithoutAlias());
   }

   @Override
   protected void addGrouping(org.hibernate.hql.ast.origin.hql.resolve.path.PropertyPath propertyPath, String collateName) {
      // collationName is ignored
      if (groupBy == null) {
         groupBy = new ArrayList<PropertyPath>(5);
      }
      groupBy.add(makePropertyPath(propertyPath));
   }

   @Override
   public void setPropertyPath(org.hibernate.hql.ast.origin.hql.resolve.path.PropertyPath propertyPath) {
      if (status == Status.DEFINING_SELECT) {
         if (propertyPath.getNodes().size() == 1 && propertyPath.getNodes().get(0).isAlias()) {
            projections.add(new PropertyPath(null, "__HSearch_This")); //todo [anistor] this is a leftover from hsearch ????   this represents the entity itself. see org.hibernate.search.ProjectionConstants
         } else {
            projections.add(makePropertyPath(propertyPath));
         }
      } else {
         this.propertyPath = propertyPath;
      }
   }

   @Override
   public void registerPersisterSpace(Tree entityName, Tree alias) {
      super.registerPersisterSpace(entityName, alias);

      targetEntityMetadata = propertyHelper.getEntityMetadata(targetTypeName);
   }

   @Override
   public FilterParsingResult<TypeMetadata> getResult() {
      BooleanExpr whereFilter = builder.build();
      BooleanExpr havingFilter = havingBuilder.build();
      return new FilterParsingResult<TypeMetadata>(whereFilter, havingFilter, targetTypeName, targetEntityMetadata, projections, groupBy, sortFields);
   }
}
