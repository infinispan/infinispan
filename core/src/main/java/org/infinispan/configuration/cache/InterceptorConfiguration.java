package org.infinispan.configuration.cache;


import org.infinispan.interceptors.base.CommandInterceptor;

/**
 * Describes a custom interceptor
 */
public class InterceptorConfiguration {

   /**
    * Positional placing of a new custom interceptor
    */
   public static enum Position {
      /**
       * Specifies that the new interceptor is placed first in the chain.
       */
      FIRST,
      /**
       * Specifies that the new interceptor is placed last in the chain.
       */
      LAST,
      /**
       * Specifies that the new interceptor can be placed anywhere, except first or last.  This is the default, if not
       * explicitly specified.
       */
      OTHER_THAN_FIRST_OR_LAST
   }
   
   private final Class<? extends CommandInterceptor> after;
   private final Class<? extends CommandInterceptor> before;
   private final CommandInterceptor interceptor;
   private final int index;
   private final Position position;
   
   InterceptorConfiguration(Class<? extends CommandInterceptor> after, Class<? extends CommandInterceptor> before,
         CommandInterceptor interceptor, int index, Position position) {
      this.after = after;
      this.before = before;
      this.interceptor = interceptor;
      this.index = index;
      this.position = position;
   }
   
   public Class<? extends CommandInterceptor> after() {
      return after;
   }
   public Class<? extends CommandInterceptor> before() {
      return before;
   }
   public CommandInterceptor interceptor() {
      return interceptor;
   }
   public int index() {
      return index;
   }
   public Position position() {
      return position;
   }
   
   public boolean first() {
      return position() == Position.FIRST;
   }

   public boolean last() {
      return position() == Position.LAST;
   }

   @Override
   public String toString() {
      return "InterceptorConfiguration{" +
            "after=" + after +
            ", before=" + before +
            ", interceptor=" + interceptor +
            ", index=" + index +
            ", position=" + position +
            '}';
   }

}
