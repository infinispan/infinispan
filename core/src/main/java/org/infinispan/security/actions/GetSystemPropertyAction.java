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

   public GetSystemPropertyAction(String propertyName) {
      this.propertyName = propertyName;
   }

   @Override
   public String run() {
      return System.getProperty(propertyName);
   }



}
