package org.infinispan.xsite.commands.remote;

import java.util.concurrent.CompletionStage;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.ByteString;
import org.infinispan.xsite.BackupReceiver;
import org.infinispan.xsite.irac.IracManager;

/**
 * A request that is sent to the remote site by {@link IracManager}.
 *
 * @author William Burns
 * @since 15.0
 */
@ProtoTypeId(ProtoStreamTypeIds.IRAC_TOUCH_KEY_REQUEST)
public class IracTouchKeyRequest extends IracUpdateKeyRequest<Boolean> {

   private Object key;

   public IracTouchKeyRequest(ByteString cacheName, Object key) {
      super(cacheName);
      this.key = key;
   }

   @ProtoFactory
   IracTouchKeyRequest(ByteString cacheName, MarshallableObject<Object> key) {
      this(cacheName, MarshallableObject.unwrap(key));
   }

   @ProtoField(2)
   MarshallableObject<Object> getKey() {
      return MarshallableObject.create(key);
   }

   @Override
   public CompletionStage<Boolean> executeOperation(BackupReceiver receiver) {
      return receiver.touchEntry(key);
   }

   @Override
   public byte getCommandId() {
      return Ids.IRAC_TOUCH;
   }
}
