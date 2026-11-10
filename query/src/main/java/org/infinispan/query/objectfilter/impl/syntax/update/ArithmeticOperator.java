package org.infinispan.query.objectfilter.impl.syntax.update;

/**
 * The arithmetic operators supported in UPDATE statement value expressions.
 *
 * @since 16.4
 */
public enum ArithmeticOperator {
   ADD("+"),
   SUBTRACT("-"),
   MULTIPLY("*"),
   DIVIDE("/"),
   MODULO("%");

   private final String symbol;

   ArithmeticOperator(String symbol) {
      this.symbol = symbol;
   }

   public String getSymbol() {
      return symbol;
   }

}
