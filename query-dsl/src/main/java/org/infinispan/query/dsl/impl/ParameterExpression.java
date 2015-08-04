package org.infinispan.query.dsl.impl;

import org.infinispan.query.dsl.Expression;

/**
 * @author anistor@redhat.com
 * @since 8.0
 */
public final class ParameterExpression implements Expression {

   private final String paramName;

   public ParameterExpression(String paramName) {
      this.paramName = paramName;
   }

   public String getParamName() {
      return paramName;
   }
}
