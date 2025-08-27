package org.infinispan.query.remote.client.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.annotations.ProtoEnumValue;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * @author anistor@redhat.com
 * @since 8.0
 */
@ProtoTypeId(ProtoStreamTypeIds.REMOTE_QUERY_ICKLE_CONTINUOUS_QUERY_RESULT)
public final class ContinuousQueryResult {

   public enum ResultType {
      @ProtoEnumValue(1)
      JOINING,
      @ProtoEnumValue(2)
      UPDATED,
      @ProtoEnumValue(3)
      LEAVING,
      /*
       * This is here so that we have a 0-valued protobuf enum
       */
      @ProtoEnumValue(0)
      UNUSED
   }

   private final ResultType resultType;

   private final byte[] key;

   private final byte[] value;

   private final Object[] projection;

   public ContinuousQueryResult(ResultType resultType, byte[] key, byte[] value, Object[] projection) {
      this.resultType = resultType;
      this.key = key;
      this.value = value;
      this.projection = projection;
   }

   // TODO re-add logic to marshalling?
   @ProtoFactory
   public ContinuousQueryResult(ResultType resultType, byte[] key, byte[] value, List<WrappedMessage> wrappedProjection) {
      this(resultType, key, value,
            wrappedProjection == null ? null : wrappedProjection.stream().map(WrappedMessage::getValue).toArray()
      );
   }

   @ProtoField(1)
   public ResultType getResultType() {
      return resultType;
   }

   @ProtoField(2)
   public byte[] getKey() {
      return key;
   }

   @ProtoField(3)
   public byte[] getValue() {
      return value;
   }

   public Object[] getProjection() {
      return projection;
   }

   @ProtoField(value = 4, name = "projection", collectionImplementation = ArrayList.class)
   List<WrappedMessage> getWrappedProjection() {
      return projection == null ? null :
            Arrays.stream(projection)
                  .map(WrappedMessage::new)
                  .collect(Collectors.toList());
   }

   @Override
   public String toString() {
      return "ContinuousQueryResult{" +
            "resultType=" + resultType +
            ", key=" + Arrays.toString(key) +
            ", value=" + Arrays.toString(value) +
            ", projection=" + Arrays.toString(projection) +
            '}';
   }
}
