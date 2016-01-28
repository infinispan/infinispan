package org.infinispan.query.dsl.impl;

import org.infinispan.query.dsl.Expression;
import org.infinispan.query.dsl.impl.logging.Log;
import org.jboss.logging.Logger;

/**
 * @author anistor@redhat.com
 * @since 8.0
 */
public final class ParameterExpression implements Expression {

   private static final Log log = Logger.getMessageLogger(Log.class, ParameterExpression.class.getName());

   private final String paramName;

   public ParameterExpression(String paramName) {
      if (paramName == null || paramName.isEmpty()) {
         throw log.parameterNameCannotBeNulOrEmpty();
      }
      this.paramName = paramName;
   }

   public String getParamName() {
      return paramName;
   }
}
