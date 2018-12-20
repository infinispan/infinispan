package org.infinispan.security.actions;

import java.security.PrivilegedAction;

/**
 * GetSystemPropertyAction.
 *
 * @author Tristan Tarrant
 * @since 8.0
 */
public class GetSystemPropertyAction implements PrivilegedAction<String> {

   private final String propertyName;
   private final String defaultValue;

   public GetSystemPropertyAction(String propertyName) {
      this.propertyName = propertyName;
      this.defaultValue = null;
   }

   public GetSystemPropertyAction(String propertyName, String defaultValue) {
      this.propertyName = propertyName;
      this.defaultValue = defaultValue;
   }

   @Override
   public String run() {
      return System.getProperty(propertyName, defaultValue);
   }
}
