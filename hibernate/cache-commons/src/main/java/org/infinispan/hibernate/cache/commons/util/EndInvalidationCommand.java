package org.infinispan.hibernate.cache.commons.util;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.hibernate.cache.commons.access.PutFromLoadValidator;
import org.infinispan.marshall.protostream.impl.MarshallableArray;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.NodeVersion;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.BlockingManager;

/**
 * Sent in commit phase (after DB commit) to remote nodes in order to stop invalidating
 * putFromLoads.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@ProtoTypeId(ProtoStreamTypeIds.HIBERNATE_INVALIDATE_COMMAND_END)
public class EndInvalidationCommand extends BaseRpcCommand {
   private final Object[] keys;
   private final Object lockOwner;

   /**
    * @param cacheName name of the cache to evict
    */
   public EndInvalidationCommand(ByteString cacheName, Object[] keys, Object lockOwner) {
      super(cacheName);
      this.keys = keys;
      this.lockOwner = lockOwner;
   }

   @ProtoFactory
   EndInvalidationCommand(ByteString cacheName, MarshallableArray<Object> keys, MarshallableObject<Object> lockOwner) {
      this(cacheName, MarshallableArray.unwrap(keys), MarshallableObject.unwrap(lockOwner));
   }

   @ProtoField(2)
   MarshallableArray<Object> getKeys() {
      return MarshallableArray.create(keys);
   }

   @ProtoField(3)
   MarshallableObject<Object> getLockOwner() {
      return MarshallableObject.create(lockOwner);
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry componentRegistry) {
      BlockingManager bm = componentRegistry.getGlobalComponentRegistry().getComponent(BlockingManager.class);
      PutFromLoadValidator putFromLoadValidator = componentRegistry.getComponent(PutFromLoadValidator.class);
      return bm.runBlocking(() -> {
         for (Object key : keys) {
            putFromLoadValidator.endInvalidatingKey(lockOwner, key);
         }
      }, "end-invalidation");
   }

   @Override
   public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) return false;
      EndInvalidationCommand that = (EndInvalidationCommand) o;
      return Objects.deepEquals(keys, that.keys) && Objects.equals(lockOwner, that.lockOwner) && Objects.equals(cacheName, that.cacheName);
   }

   @Override
   public int hashCode() {
      return Objects.hash(Arrays.hashCode(keys), lockOwner, cacheName);
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public NodeVersion supportedSince() {
      return NodeVersion.SIXTEEN;
   }

   @Override
   public String toString() {
      return "EndInvalidationCommand{" + "cacheName=" + cacheName +
            ", keys=" + Arrays.toString(keys) +
            ", sessionTransactionId=" + lockOwner +
            '}';
   }
}
