package org.infinispan.server.resp.commands.generic;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.server.core.iteration.IterationManager;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.commands.iteration.BaseIterationCommand;

/**
 * SCAN
 *
 * @see <a href="https://redis.io/commands/scan/">SCAN</a>
 * @since 15.0
 */
public class SCAN extends BaseIterationCommand {

   public SCAN() {
      super(-2, 0, 0, 0, AclCategory.KEYSPACE.mask() | AclCategory.READ.mask() | AclCategory.SLOW.mask());
   }

   @Override
   protected IterationManager retrieveIterationManager(Resp3Handler handler) {
      return handler.respServer().getIterationManager();
   }

   @Override
   protected String cursor(List<byte[]> raw) {
      return new String(raw.get(0), StandardCharsets.US_ASCII);
   }

   @Override
   protected List<byte[]> writeResponse(List<CacheEntry> response) {
      List<byte[]> output = new ArrayList<>(response.size());
      for (CacheEntry<?, ?> entry : response) {
         output.add((byte[]) entry.getKey());
      }
      return output;
   }
}
