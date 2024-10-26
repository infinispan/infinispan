package org.infinispan.server.resp.commands.generic;

/**
 * EXPIRETIME
 *
 * @see <a href="https://redis.io/commands/expiretime/>EXPIRETIME</a>
 * @since 15.0
 **/
public class EXPIRETIME extends TTL {
   public EXPIRETIME() {
      super(ExpirationOption.UNIX_TIME, ExpirationOption.SECONDS);
   }
}
