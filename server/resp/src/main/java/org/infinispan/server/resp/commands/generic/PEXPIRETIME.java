package org.infinispan.server.resp.commands.generic;

/**
 * PEXPIRETIME
 *
 * @see <a href="https://redis.io/commands/pexpiretime/">PEXPIRETIME</a>
 * @since 15.0
 */
public class PEXPIRETIME extends TTL {
   public PEXPIRETIME() {
      super(ExpirationOption.UNIX_TIME);
   }
}
