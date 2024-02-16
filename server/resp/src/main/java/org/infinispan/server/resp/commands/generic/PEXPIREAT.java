package org.infinispan.server.resp.commands.generic;

/**
 * PEXPIREAT Resp Command, like {@link EXPIREAT} Unix time expiration in milliseconds
 *
 * @link <a href="https://redis.io/commands/pexpireat/">PEXPIREAT</a>
 * @since 15.0
 */
public class PEXPIREAT extends EXPIRE {
   public PEXPIREAT() {
      super(true, false);
   }
}
