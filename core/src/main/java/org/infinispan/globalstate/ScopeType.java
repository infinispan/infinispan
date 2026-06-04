package org.infinispan.globalstate;

/**
 * Types of scopes used in the global state cache.
 */
public enum ScopeType {
   CACHE("cache"),
   TEMPLATE("template"),
   CONTAINER("container");

   private final String value;

   ScopeType(String value) {
      this.value = value;
   }

   @Override
   public String toString() {
      return value;
   }

   public static ScopeType fromString(String scope) {
      for (ScopeType type : values()) {
         if (type.value.equals(scope)) {
            return type;
         }
      }
      return null;
   }
}
