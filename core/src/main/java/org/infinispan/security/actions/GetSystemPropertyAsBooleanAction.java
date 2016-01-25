package org.infinispan.security.actions;

import java.security.PrivilegedAction;

/**
 * Wrapper for {@code Boolean.getBoolean(propertyName)}
 *
 * @author Dan Berindei
 * @since 8.2
 */
public class GetSystemPropertyAsBooleanAction implements PrivilegedAction<Boolean> {

   private final String propertyName;

   public GetSystemPropertyAsBooleanAction(String propertyName) {
      this.propertyName = propertyName;
   }

   @Override
   public Boolean run() {
      return Boolean.getBoolean(propertyName);
   }
}
