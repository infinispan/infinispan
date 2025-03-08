package org.infinispan.server.resp.scripting;

public enum ScriptFlags {
   /**
    * this flag indicates that the script only reads data but never writes.
    */
   NO_WRITES(1),
   /**
    * use this flag to allow a script to execute when the server is out of memory (OOM).
    */
   ALLOW_OOM(2),
   /**
    * allow running the script on a stale replica.
    */
   ALLOW_STALE(4),
   /**
    * return an error if the script is executed in clustered mode.
    */
   NO_CLUSTER(8),
   /**
    * EVAL Script backwards compatible behavior, no shebang provided
    */
   EVAL_COMPAT_MODE(16),
   /**
    * allow the script to access keys from multiple slots.
    */
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
