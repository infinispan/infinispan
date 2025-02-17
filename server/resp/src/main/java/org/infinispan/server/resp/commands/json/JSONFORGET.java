package org.infinispan.server.resp.commands.json;

/**
 * JSON.FORGET
 *
 * @see <a href="https://redis.io/commands/json.forget/">JSON.FORGET</a>
 *
 * @since 15.2
 */
public class JSONFORGET extends JSONDEL {
    // This command is an alias of JSON.DEL
   public JSONFORGET() {
      super("JSON.FORGET", -2, 1, 1, 1);
   }
}
