package org.infinispan.server.resp.commands.json;

/**
 * JSON.STRLEN
 *
 * @see <a href="https://redis.io/commands/json.strlen/">JSON.STRLEN</a>
 * @since 15.2
 */
public class JSONSTRLEN extends JSONLEN {
   public JSONSTRLEN() {
      super("JSON.STRLEN");
   }
}
