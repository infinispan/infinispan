package org.infinispan.server.resp;

/**
 * @since 15.0
 **/
public enum RespTypes {
   none,
   hash,
   list,
   set,
   stream,
   string,
   zset,
   // Not a real Resp type
   unknown
}
