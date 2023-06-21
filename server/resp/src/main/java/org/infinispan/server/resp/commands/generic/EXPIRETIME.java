package org.infinispan.server.resp.commands.generic;

/**
 * @since 15.0
 **/
public class EXPIRETIME extends TTL {
   public EXPIRETIME() {
      super(true, false);
   }
}
