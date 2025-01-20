package org.infinispan.server.resp.scripting;

public enum ScriptFlags {
   NO_WRITES(1),
   ALLOW_OOM(2),
   ALLOW_STALE(4),
   NO_CLUSTER(8),
   EVAL_COMPAT_MODE(16),
   ALLOW_CROSS_SLOT_KEYS(32);

   private final long value;

   ScriptFlags(long value) {
      this.value = value;
   }

   public boolean isSet(long mask) {
      return (mask & value) == value;
   }

   public long value() {
      return value;
   }
}
