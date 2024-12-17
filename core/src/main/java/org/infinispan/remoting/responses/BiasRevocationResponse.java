package org.infinispan.remoting.responses;

import java.util.Collection;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.MarshallableArray;
import org.infinispan.marshall.protostream.impl.MarshallableCollection;
import org.infinispan.marshall.protostream.impl.MarshallableMap;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.Address;

@ProtoTypeId(ProtoStreamTypeIds.BIAS_REVOCATION_RESPONSE)
public class BiasRevocationResponse extends SuccessfulResponse {

   public BiasRevocationResponse(Object responseValue, Collection<Address> waitFor) {
      super(MarshallableObject.create(responseValue), MarshallableCollection.create(waitFor), null, null, null);
   }

   @ProtoFactory
   BiasRevocationResponse(MarshallableObject<?> object, MarshallableCollection<?> collection,
                          MarshallableMap<?, ?> map, MarshallableArray<?> array, byte[] bytes) {
      super(object, collection, map, array, bytes);
   }

   public Collection<Address> getWaitList() {
      return getResponseCollection();
   }
}
