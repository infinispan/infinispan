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
 * A {@link CacheRpcCommand} implementation to forward the rollback request from a client to the member that run the
 * transaction.
 *
 * @author Pedro Ruivo
 * @since 9.1
 */
public class ForwardRollbackCommand extends BaseRpcCommand {

   private XidImpl xid;
   private long timeout;
   private AdvancedCache<byte[], byte[]> cache;

   public ForwardRollbackCommand(ByteString cacheName) {
      super(cacheName);
   }

   public ForwardRollbackCommand(ByteString cacheName, XidImpl xid, long timeout) {
      super(cacheName);
      this.xid = xid;
      this.timeout = timeout;
   }

   @Override
   public byte getCommandId() {
      return Ids.FORWARD_ROLLBACK;
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
      Util.rollbackLocalTransaction(cache, xid, timeout);
      return null;
   }

   public void inject(EmbeddedCacheManager cacheManager) {
      this.cache = cacheManager.<byte[], byte[]>getCache(cacheName.toString()).getAdvancedCache();
   }

   @Override
   public String toString() {
      return "ForwardRollbackCommand{" +
            "cacheName=" + cacheName +
            ", xid=" + xid +
            '}';
   }
}
