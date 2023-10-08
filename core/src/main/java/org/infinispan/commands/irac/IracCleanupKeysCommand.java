package org.infinispan.commands.irac;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.marshall.protostream.impl.MarshallableCollection;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.ByteString;
import org.infinispan.xsite.irac.IracManager;
import org.infinispan.xsite.irac.IracManagerKeyInfo;

/**
 * Sends a cleanup request from the primary owner to the backup owners.
 * <p>
 * Sent after a successful update of all remote sites.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
@ProtoTypeId(ProtoStreamTypeIds.IRAC_CLEANUP_KEYS_COMMAND)
public class IracCleanupKeysCommand extends BaseRpcCommand {

   public static final byte COMMAND_ID = 122;

   private Collection<IracManagerKeyInfo> cleanup;

   public IracCleanupKeysCommand(ByteString cacheName, Collection<IracManagerKeyInfo> cleanup) {
      super(cacheName);
      this.cleanup = cleanup;
   }

   @ProtoFactory
   IracCleanupKeysCommand(ByteString cacheName, MarshallableCollection<IracManagerKeyInfo> cleanup) {
      this(cacheName, MarshallableCollection.unwrap(cleanup));
   }

   @ProtoField(2)
   MarshallableCollection<IracManagerKeyInfo> getCleanup() {
      return MarshallableCollection.create(cleanup);
   }

   @Override
   public CompletableFuture<Object> invokeAsync(ComponentRegistry componentRegistry) {
      IracManager manager = componentRegistry.getIracManager().running();
      cleanup.forEach(manager::removeState);
      return CompletableFutures.completedNull();
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public String toString() {
      return "IracCleanupKeysCommand{" +
            "cacheName=" + cacheName +
            ", cleanup=" + Util.toStr(cleanup) +
            '}';
   }
}
