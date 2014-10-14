package org.infinispan.cli.interpreter.result;

/**
 *
 * EmptyResult. A result returned by operations which do not produce any output
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public final class EmptyResult implements Result {
   public static final EmptyResult RESULT = new EmptyResult();

   private EmptyResult() {}

   @Override
   public String getResult() {
      return null;
   };
}
