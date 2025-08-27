package org.infinispan.server.resp.commands.generic;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.server.core.iteration.IterationManager;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.commands.iteration.BaseIterationCommand;

/**
 * KEYS
 *
 * @see <a href="https://redis.io/commands/keys/">KEYS</a>
 * @since 15.0
 */
public class KEYS extends BaseIterationCommand {

   public KEYS() {
      super(2, 0, 0, 0, AclCategory.KEYSPACE.mask() | AclCategory.READ.mask() | AclCategory.SLOW.mask() | AclCategory.DANGEROUS.mask());
   }

   @Override
   protected boolean writeCursor() {
      return false;
   }

   @Override
   protected IterationManager retrieveIterationManager(Resp3Handler handler) {
      return handler.respServer().getIterationManager();
   }

   @Override
   protected String cursor(List<byte[]> raw) {
      return "0";
   }

   @Override
   protected byte[] getMatch(List<byte[]> arguments) {
      return arguments.get(0);
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
