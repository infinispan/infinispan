package org.infinispan.query.dsl.embedded.impl.jpalucene;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.hibernate.search.bridge.FieldBridge;
import org.hibernate.search.bridge.builtin.NumericFieldBridge;
import org.hibernate.search.bridge.builtin.impl.NullEncodingTwoWayFieldBridge;
import org.hibernate.search.query.dsl.BooleanJunction;
import org.hibernate.search.query.dsl.FieldCustomization;
import org.hibernate.search.query.dsl.QueryBuilder;
import org.hibernate.search.query.dsl.QueryContextBuilder;
import org.hibernate.search.query.dsl.impl.FieldBridgeCustomization;
import org.infinispan.objectfilter.impl.hql.FilterParsingResult;
import org.infinispan.objectfilter.impl.syntax.AggregationExpr;
import org.infinispan.objectfilter.impl.syntax.AndExpr;
import org.infinispan.objectfilter.impl.syntax.BooleanExpr;
import org.infinispan.objectfilter.impl.syntax.ComparisonExpr;
import org.infinispan.objectfilter.impl.syntax.ConstantBooleanExpr;
import org.infinispan.objectfilter.impl.syntax.ConstantValueExpr;
import org.infinispan.objectfilter.impl.syntax.IsNullExpr;
import org.infinispan.objectfilter.impl.syntax.LikeExpr;
import org.infinispan.objectfilter.impl.syntax.NotExpr;
import org.infinispan.objectfilter.impl.syntax.OrExpr;
import org.infinispan.objectfilter.impl.syntax.PropertyValueExpr;
import org.infinispan.objectfilter.impl.syntax.Visitor;
import org.infinispan.objectfilter.impl.util.StringHelper;

import java.util.Map;
import java.util.regex.Pattern;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
class LuceneQueryMaker implements Visitor<Query, Query> {

   private static final String LUCENE_SINGLE_CHARACTER_WILDCARD = "?";
   private static final String LUCENE_MULTIPLE_CHARACTERS_WILDCARD = "*";

   private static final Pattern MULTIPLE_CHARACTERS_WILDCARD_PATTERN = Pattern.compile("%");
   private static final Pattern SINGLE_CHARACTER_WILDCARD_PATTERN = Pattern.compile("_");

   private final QueryContextBuilder queryContextBuilder;
   private final EntityNamesResolver entityNamesResolver;
   private final JPALuceneTransformer.FieldBridgeProvider fieldBridgeProvider;
   private final Map<String, Object> namedParameters;

   private QueryBuilder queryBuilder;
   private String entityType;

   LuceneQueryMaker(QueryContextBuilder queryContextBuilder, EntityNamesResolver entityNamesResolver,
                    JPALuceneTransformer.FieldBridgeProvider fieldBridgeProvider, Map<String, Object> namedParameters) {
      this.queryContextBuilder = queryContextBuilder;
      this.entityNamesResolver = entityNamesResolver;
      this.fieldBridgeProvider = fieldBridgeProvider;
      this.namedParameters = namedParameters;
   }

   <TypeMetadata> LuceneQueryParsingResult<TypeMetadata> transform(FilterParsingResult<TypeMetadata> parsingResult) {
      Query query = makeQuery(parsingResult.getTargetEntityName(), parsingResult.getWhereClause());
      Sort sort = makeSort(parsingResult.getSortFields());
      return new LuceneQueryParsingResult<>(query, parsingResult.getTargetEntityName(), parsingResult.getTargetEntityMetadata(), parsingResult.getProjections(), sort);
   }

   private Query makeQuery(String entityType, BooleanExpr expr) {
      Class<?> targetedType = entityNamesResolver.getClassFromName(entityType);
      if (targetedType == null) {
         throw new IllegalStateException("Unknown entity name " + entityType);
      }
      this.entityType = entityType;
      queryBuilder = queryContextBuilder.forEntity(targetedType).get();
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
         FieldBridge fieldBridge = fieldBridgeProvider.getFieldBridge(entityType, sf.getPath().getPath());
         if (fieldBridge instanceof NullEncodingTwoWayFieldBridge) {
            fieldBridge = ((NullEncodingTwoWayFieldBridge) fieldBridge).unwrap();
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
      BooleanJunction<BooleanJunction> booleanJunction = queryBuilder.bool();
      for (BooleanExpr c : andExpr.getChildren()) {
         if (c instanceof NotExpr) {
            // minor optimization: unwrap negated predicates and add child directly to this predicate
            c = ((NotExpr) c).getChild();
            Query transformedChild = c.acceptVisitor(this);
            booleanJunction.must(transformedChild).not();
         } else {
            Query transformedChild = c.acceptVisitor(this);
            booleanJunction.must(transformedChild);
         }
      }
      return booleanJunction.createQuery();
   }

   @Override
   public Query visit(IsNullExpr isNullExpr) {
      PropertyValueExpr propertyValueExpr = (PropertyValueExpr) isNullExpr.getChild();
      String[] propertyPath = propertyValueExpr.getPropertyPath();
      return applyFieldBridge(propertyPath,
            queryBuilder.keyword().onField(StringHelper.join(propertyPath))).matching(null).createQuery();
   }

   @Override
   public Query visit(ComparisonExpr comparisonExpr) {
      PropertyValueExpr propertyValueExpr = (PropertyValueExpr) comparisonExpr.getLeftChild();
      ConstantValueExpr constantValueExpr = (ConstantValueExpr) comparisonExpr.getRightChild();
      Comparable value = constantValueExpr.getConstantValueAs(propertyValueExpr.getPrimitiveType(), namedParameters);
      String[] propertyPath = propertyValueExpr.getPropertyPath();
      String path = StringHelper.join(propertyPath);
      switch (comparisonExpr.getComparisonType()) {
         case NOT_EQUAL:
            Query q = applyFieldBridge(propertyPath, queryBuilder.keyword().onField(path))
                  .matching(value).createQuery();
            return queryBuilder.bool().must(q).not().createQuery();
         case EQUAL:
            return applyFieldBridge(propertyPath, queryBuilder.keyword().onField(path))
                  .matching(value).createQuery();
         case LESS:
            return applyFieldBridge(propertyPath, queryBuilder.range().onField(path))
                  .below(value).excludeLimit().createQuery();
         case LESS_OR_EQUAL:
            return applyFieldBridge(propertyPath, queryBuilder.range().onField(path))
                  .below(value).createQuery();
         case GREATER:
            return applyFieldBridge(propertyPath, queryBuilder.range().onField(path))
                  .above(value).excludeLimit().createQuery();
         case GREATER_OR_EQUAL:
            return applyFieldBridge(propertyPath, queryBuilder.range().onField(path))
                  .above(value).createQuery();
         default:
            throw new IllegalStateException("Unexpected comparison type: " + comparisonExpr.getComparisonType());
      }
   }

   @Override
   public Query visit(LikeExpr likeExpr) {
      PropertyValueExpr propertyValueExpr = (PropertyValueExpr) likeExpr.getChild();
      String[] propertyPath = propertyValueExpr.getPropertyPath();
      String patternValue = MULTIPLE_CHARACTERS_WILDCARD_PATTERN.matcher(likeExpr.getPattern()).replaceAll(LUCENE_MULTIPLE_CHARACTERS_WILDCARD);
      patternValue = SINGLE_CHARACTER_WILDCARD_PATTERN.matcher(patternValue).replaceAll(LUCENE_SINGLE_CHARACTER_WILDCARD);
      return applyFieldBridge(propertyPath, queryBuilder.keyword().wildcard().onField(StringHelper.join(propertyPath)))
            .matching(patternValue).createQuery();
   }

   @Override
   public Query visit(ConstantBooleanExpr constantBooleanExpr) {
      throw new IllegalStateException("This node type should not be visited");
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

   private <F extends FieldCustomization> F applyFieldBridge(String[] propertyPath, F f) {
      FieldBridge fieldBridge = fieldBridgeProvider.getFieldBridge(entityType, propertyPath);
      if (fieldBridge != null) {
         ((FieldBridgeCustomization) f).withFieldBridge(fieldBridge);
         f.ignoreAnalyzer();
      }
      return f;
   }
}
