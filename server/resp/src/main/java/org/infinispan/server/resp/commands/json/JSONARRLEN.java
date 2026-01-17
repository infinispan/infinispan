package org.infinispan.server.resp.commands.json;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.RespUtil;
import org.infinispan.server.resp.json.EmbeddedJsonCache;

/**
 * JSON.ARRLEN
 *
 * @see <a href="https://redis.io/commands/json.arrlen/">JSON.ARRLEN</a>
 * @since 15.2
 */
public class JSONARRLEN extends JSONLEN {
   public JSONARRLEN() {
      super("JSON.ARRLEN", AclCategory.JSON.mask() | AclCategory.READ.mask() | AclCategory.SLOW.mask());
   }

   @Override
   protected CompletionStage<List<Long>> len(EmbeddedJsonCache ejc, byte[] key, byte[] path) {
      return ejc.arrLen(key, path);
   }

   @Override
   protected void raiseTypeError(byte[] path) {
      throw new RuntimeException("Path '" + RespUtil.ascii(path) + "' is not an array");
   }
}
