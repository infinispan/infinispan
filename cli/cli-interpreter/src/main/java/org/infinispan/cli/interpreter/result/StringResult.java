package org.infinispan.cli.interpreter.result;

/**
 * @author Tristan Tarrant
 * @since 5.2
 */
public class StringResult implements Result {
   final String s;

   public StringResult(String s) {
      this.s = s;
   }

   public StringResult(String format, Object... args) {
      this.s = String.format(format, args);
   }

   @Override
   public String getResult() {
      return s;
   }


}
