package org.infinispan.server.hotrod.command.tx;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.command.Ids;
import org.infinispan.server.hotrod.tx.operation.Util;
import org.infinispan.util.ByteString;

/**
 * A {@link CacheRpcCommand} implementation to forward the commit request from a client to the member that run the
 * transaction.
 *
 * @author Pedro Ruivo
 * @since 9.1
 */
public class ForwardCommitCommand extends BaseRpcCommand {

   private XidImpl xid;
   private long timeout;
   private AdvancedCache<byte[], byte[]> cache;

   public ForwardCommitCommand(ByteString cacheName) {
      super(cacheName);
   }

   public ForwardCommitCommand(ByteString cacheName, XidImpl xid, long timeout) {
      super(cacheName);
      this.xid = xid;
      this.timeout = timeout;
   }

   @Override
   public byte getCommandId() {
      return Ids.FORWARD_COMMIT;
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public boolean canBlock() {
      return true;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      XidImpl.writeTo(output, xid);
      output.writeLong(timeout);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException {
      xid = XidImpl.readFrom(input);
      timeout = input.readLong();
   }

   @Override
   public Object invoke() throws Throwable {
      Util.commitLocalTransaction(cache, xid, timeout);
      return null;
   }

   @Override
   public String toString() {
      return "ForwardCommitCommand{" +
            "cacheName=" + cacheName +
            ", xid=" + xid +
            '}';
   }

   public void inject(EmbeddedCacheManager cacheManager) {
      this.cache = cacheManager.<byte[], byte[]>getCache(cacheName.toString()).getAdvancedCache();
   }
}
