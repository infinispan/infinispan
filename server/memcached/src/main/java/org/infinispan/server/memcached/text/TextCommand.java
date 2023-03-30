package org.infinispan.server.memcached.text;

/**
 * @since 15.0
 **/
public enum TextCommand {
   // Text commands
   set,
   get,
   gets,
   add,
   replace,
   append,
   prepend,
   incr,
   decr,
   delete,
   gat,
   gats,
   touch,
   flush_all,
   cas,
   stats,
   verbosity,
   version,
   quit,
   // Meta commands
   mg,
   ms,
   md,
   ma,
   mn,
   me
}
