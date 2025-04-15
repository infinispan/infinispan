package org.infinispan.server.resp;

import java.util.ArrayList;
import java.util.List;

public enum AclCategory {
   KEYSPACE(1L),
   READ(1L << 1),
   WRITE(1L << 2),
   SET(1L << 3),
   SORTEDSET(1L << 4),
   LIST(1L << 5),
   HASH(1L << 6),
   STRING(1L << 7),
   BITMAP(1L << 8),
   HYPERLOGLOG(1L << 9),
   GEO(1L << 10),
   STREAM(1L << 11),
   PUBSUB(1L << 12),
   ADMIN(1L << 13),
   FAST(1L << 14),
   SLOW(1L << 15),
   BLOCKING(1L << 16),
   DANGEROUS(1L << 17),
   CONNECTION(1L << 18),
   TRANSACTION(1L << 19),
   SCRIPTING(1L << 20),
   BLOOM(1L << 21),
   CUCKOO(1L << 22),
   CMS(1L << 23),
   TOPK(1L << 24),
   TDIGEST(1L << 25),
   SEARCH(1L << 26),
   TIMESERIES(1L << 27),
   JSON(1L << 28);

   private final long mask;

   AclCategory(long mask) {
      this.mask = mask;
   }

   public long mask() {
      return mask;
   }

   public boolean matches(long mask) {
      return (mask & this.mask) == this.mask;
   }

   public String toString() {
      return "@" + name().toLowerCase();
   }

   public static List<String> aclNames(long mask) {
      List<String> names = new ArrayList<>();
      for (AclCategory c : AclCategory.values()) {
         if (c.matches(mask)) {
            names.add(c.toString());
         }
      }
      return names;
   }
}
