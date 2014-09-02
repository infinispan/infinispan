package org.infinispan.server.cli.handlers;

/**
 * The commands interpreted by the Infinispan CLI interpreter with their handlers.
 *
 * @author Pedro Ruivo
 * @since 6.1
 */
public enum CacheCommand {
   ABORT("abort", -1),
   BEGIN("begin", 1),
   CACHE("cache"),
   CLEARCACHE("clearcache"),
   COMMIT("commit", -1),
   CONTAINER("container"),
   CREATE("create"),
   DENY("deny"),
   ENCODING("encoding"),
   END("end", -1),
   EVICT("evict"),
   GET("get"),
   GRANT("grant"),
   INFO("info"),
   LOCATE("locate"),
   PUT("put"),
   REMOVE("remove"),
   REPLACE("replace"),
   ROLES("roles"),
   ROLLBACK("rollback", -1),
   SITE("site"),
   START("start", 1),
   STATS("stats"),
   UPGRADE("upgrade"),
   VERSION("version");

   private final String name;
   private final int nesting;

   CacheCommand(String name) {
      this(name, 0);
   }

   CacheCommand(String name, int nesting) {
      this.name = name;
      this.nesting = nesting;
   }

   public String getName() {
      return name;
   }

   public int getNesting() {
      return nesting;
   }

}
