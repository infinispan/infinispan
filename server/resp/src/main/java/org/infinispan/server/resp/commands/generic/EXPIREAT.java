package org.infinispan.server.resp.commands.generic;

/**
 * @since 15.0
 **/
public class EXPIREAT extends EXPIRE {
   public EXPIREAT() {
      super(true);
   }
}
