package org.infinispan.security.actions;

import java.security.PrivilegedAction;

/**
 * Wrapper for {@code Integer.getInteger(propertyName, defaultValue)}.
 *
 * @author Dan Berindei
 * @since 8.2
 */
public class GetSystemPropertyAsIntegerAction implements PrivilegedAction<Integer> {

   private final String propertyName;
   private final int defaultValue;

   public GetSystemPropertyAsIntegerAction(String propertyName, int defaultValue) {
      this.propertyName = propertyName;
      this.defaultValue = defaultValue;
   }

   @Override
   public Integer run() {
      return Integer.getInteger(propertyName, defaultValue);
   }
}
