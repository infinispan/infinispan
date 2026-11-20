package org.infinispan.query.objectfilter.impl.ql;

import java.util.Collections;

import org.infinispan.query.objectfilter.impl.syntax.ConstantValueExpr;
import org.infinispan.query.objectfilter.impl.syntax.parser.TypeDescriptor;
import org.infinispan.query.objectfilter.impl.syntax.update.UpdateValueExpr;

/**
 * A projection that evaluates an expression against the entity at query
 * execution time, e.g. {@code concat(name, ' - ', description)} in
 * {@code SELECT concat(name, ' - ', description) FROM entity}.
 * <p>
 * Unlike simple property projections, this cannot be pushed down to the
 * search index; the entity must be fetched and the expression evaluated
 * in-memory using the {@code QueryFunctionRegistry}.
 * <p>
 * The expression uses the same value-expression representation as
 * {@code UPDATE} statement values: an {@link UpdateValueExpr} tree, a
 * {@link ConstantValueExpr.ParamPlaceholder} named-parameter reference or
 * a raw constant. SELECT projections and UPDATE values therefore share
 * one parser, one evaluator and one set of semantics.
 *
 * @since 16.4
 */
public final class ExpressionProjection<TypeMetadata> extends PropertyPath<TypeDescriptor<TypeMetadata>> {

   public static final String EXPRESSION_PROPERTY_NAME = "__Expression";

   private final Object expression;

   public ExpressionProjection(Object expression) {
      super(Collections.singletonList(new PropertyReference<>(EXPRESSION_PROPERTY_NAME, null, true)));
      this.expression = expression;
   }

   /**
    * The projected expression: an {@link UpdateValueExpr} tree, a
    * {@link ConstantValueExpr.ParamPlaceholder} or a raw constant.
    */
   public Object getExpression() {
      return expression;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;
      ExpressionProjection<?> that = (ExpressionProjection<?>) o;
      return expression.equals(that.expression);
   }

   @Override
   public int hashCode() {
      return 31 * super.hashCode() + expression.hashCode();
   }

   @Override
   public String toString() {
      if (expression instanceof UpdateValueExpr expr) {
         StringBuilder sb = new StringBuilder();
         expr.appendQueryString(sb);
         return sb.toString();
      }
      return String.valueOf(expression);
   }
}
