package org.infinispan.server.resp.commands.generic;

/**
 * PEXPIRE
 *
 * @see <a href="https://redis.io/commands/pexpire/">PEXPIRE</a>
 * @since 15.0
 */
public class PEXPIRE extends EXPIRE {

   public PEXPIRE() {
      super(false, false);
   }
}
