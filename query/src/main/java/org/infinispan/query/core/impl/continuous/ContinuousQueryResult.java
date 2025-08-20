package org.infinispan.query.core.impl.continuous;

import java.util.Arrays;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.MarshallableArray;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.Proto;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * @author anistor@redhat.com
 * @since 8.0
 */
@ProtoTypeId(ProtoStreamTypeIds.ICKLE_CONTINOUS_QUERY_RESULT)
public final class ContinuousQueryResult<V> {

   @Proto
   @ProtoTypeId(ProtoStreamTypeIds.ICKLE_CONTINOUS_QUERY_RESULT_TYPE)
   public enum ResultType {
      JOINING,
      UPDATED,
      LEAVING
   }

   private final ResultType resultType;

   private final V value;

   private final Object[] projection;

   ContinuousQueryResult(ResultType resultType, V value, Object[] projection) {
      this.resultType = resultType;
      this.value = value;
      this.projection = projection;
   }

   @ProtoFactory
   ContinuousQueryResult(ResultType resultType, MarshallableObject<V> wrappedValue, MarshallableArray<Object> wrappedProjection) {
      this(resultType, MarshallableObject.unwrap(wrappedValue), MarshallableArray.unwrap(wrappedProjection));
   }

   @ProtoField(1)
   public ResultType getResultType() {
      return resultType;
   }

   @ProtoField(number = 2, name = "value")
   MarshallableObject<V> getWrappedValue() {
      return resultType != ResultType.LEAVING && projection == null ? MarshallableObject.create(value) : null;
   }

   @ProtoField(number = 3, name = "projection")
   MarshallableArray<Object> getWrappedProjection() {
      return resultType != ResultType.LEAVING ? MarshallableArray.create(projection) : null;
   }

   public V getValue() {
      return value;
   }

   public Object[] getProjection() {
      return projection;
   }

   @Override
   public String toString() {
      return "ContinuousQueryResult{" +
            "resultType=" + resultType +
            ", value=" + value +
            ", projection=" + Arrays.toString(projection) +
            '}';
   }
}
