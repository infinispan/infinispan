package org.infinispan.security;

/**
 * AuditContext.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public enum AuditContext {
   CACHE("cache"),
   CACHEMANAGER("container"),
   COUNTER("counter"),
   SERVER("server");

   private final String name;

   AuditContext(String name) {
      this.name = name;
   }

   public String toString() {
      return name;
   }
}
