package org.infinispan.scripting.impl;

/**
 * SystemBindings.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
public enum SystemBindings {
   CACHE_MANAGER("cacheManager"), CACHE("cache"), SCRIPTING_MANAGER("scriptingManager");

   private final String bindingName;

   private SystemBindings(String bindingName) {
      this.bindingName = bindingName;
   }

   @Override
   public String toString() {
      return bindingName;
   }
}
