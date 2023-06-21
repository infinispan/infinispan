package org.infinispan.server.resp.commands.generic;

/**
 * @since 15.0
 **/
public class PEXPIRETIME extends TTL {
   public PEXPIRETIME() {
      super(true, true);
   }
}
