package org.infinispan.transaction;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.marshall.protostream.impl.MarshallableThrowable;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Thrown when a write skew is detected
 *
 * @author Manik Surtani
 * @since 5.1
 */
@ProtoTypeId(ProtoStreamTypeIds.EXCEPTION_WRITE_SKEW)
public class WriteSkewException extends CacheException {

   private final Object key;

   public WriteSkewException(String msg, Object key) {
      super(msg);
      this.key = key;
   }

   public WriteSkewException(String msg, Throwable cause, Object key) {
      super(msg, cause);
      this.key = key;
   }

   @ProtoFactory
   WriteSkewException(String msg, MarshallableObject<?> wrappedKey, MarshallableThrowable wrappedCause) {
      this(msg, MarshallableThrowable.unwrap(wrappedCause), MarshallableObject.unwrap(wrappedKey));
   }

   public final Object getKey() {
      return key;
   }

   @ProtoField(value = 1, name = "message")
   String getMsg() {
      return super.getMessage();
   }

   @ProtoField(2)
   MarshallableObject<?> getWrappedKey() {
      return MarshallableObject.create(key);
   }

   @ProtoField(3)
   MarshallableThrowable getWrappedCause() {
      return MarshallableThrowable.create(getCause());
   }
}
