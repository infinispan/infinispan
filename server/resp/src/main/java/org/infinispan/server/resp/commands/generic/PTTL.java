package org.infinispan.server.resp.commands.generic;

/**
 * PTTL Resp Command
 * <a href="https://redis.io/commands/pttl/">pttl</a>
 * @since 15.0
 */
public class PTTL extends TTL {

   public PTTL() {
      super(false, true);
   }
}
