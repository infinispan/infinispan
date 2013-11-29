package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.AbstractTypedPropertiesConfiguration;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.interceptors.base.CommandInterceptor;

/**
 * Describes a custom interceptor
 */
public class InterceptorConfiguration extends AbstractTypedPropertiesConfiguration {

   /**
    * Positional placing of a new custom interceptor
    */
   public static enum Position {
      /**
       * Specifies that the new interceptor is placed first in the chain.
       */
      FIRST,
      /**
       * Specifies that the new interceptor is placed last in the chain. The new interceptor is added right before the
       * last interceptor in the chain. The very last interceptor is owned by Infinispan and cannot be replaced.
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
         CommandInterceptor interceptor, int index, Position position, TypedProperties properties) {
      super(properties);
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
      return "InterceptorConfiguration [after=" + after + ", before=" + before + ", interceptor=" + interceptor + ", index=" + index + ", position=" + position + ", properties="
            + properties() + "]";
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      InterceptorConfiguration that = (InterceptorConfiguration) o;

      if (index != that.index) return false;
      if (after != null ? !after.equals(that.after) : that.after != null)
         return false;
      if (before != null ? !before.equals(that.before) : that.before != null)
         return false;
      if (interceptor != null ? !interceptor.equals(that.interceptor) : that.interceptor != null)
         return false;
      if (position != that.position) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = after != null ? after.hashCode() : 0;
      result = 31 * result + (before != null ? before.hashCode() : 0);
      result = 31 * result + (interceptor != null ? interceptor.hashCode() : 0);
      result = 31 * result + index;
      result = 31 * result + (position != null ? position.hashCode() : 0);
      return result;
   }

}
