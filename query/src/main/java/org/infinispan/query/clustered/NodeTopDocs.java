package org.infinispan.query.clustered;

import org.apache.lucene.search.TopDocs;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.MarshallableArray;
import org.infinispan.marshall.protostream.impl.WrappedMessages;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;

/**
 * A TopDocs with an array with keys of each result.
 *
 * @author Israel Lacerra &lt;israeldl@gmail.com&gt;
 * @since 5.1
 */
@ProtoTypeId(ProtoStreamTypeIds.NODE_TOP_DOCS)
public final class NodeTopDocs {

   public final Address address;
   public final TopDocs topDocs;
   public int totalHitCount;
   public boolean countIsExact;
   public final Object[] keys;
   public final Object[] projections;

   public NodeTopDocs(Address address, TopDocs topDocs, int totalHitCount, boolean countIsExact, Object[] keys,
                      Object[] projections) {
      this.address = address;
      this.topDocs = topDocs;
      this.totalHitCount = totalHitCount;
      this.countIsExact = countIsExact;
      this.keys = keys;
      this.projections = projections;
   }

   @ProtoFactory
   NodeTopDocs(JGroupsAddress address, WrappedMessage topDocs, int totalHitCount, boolean countIsExact, MarshallableArray<Object> keys,
               MarshallableArray<Object> projections) {
      this(
            address, WrappedMessages.unwrap(topDocs), totalHitCount, countIsExact,
            MarshallableArray.unwrap(keys),
            MarshallableArray.unwrap(projections)
      );
   }

   @ProtoField(value = 1, javaType = JGroupsAddress.class)
   Address getAddress() {
      return address;
   }

   @ProtoField(2)
   WrappedMessage getTopDocs() {
      // We must use a WrappedMessage here to allow for inheritance as this can either be TopDocs or TopFieldDocs
      return WrappedMessages.orElseNull(topDocs);
   }

   @ProtoField(3)
   int totalHitCount() {
      return totalHitCount;
   }

   @ProtoField(4)
   boolean countIsExact() {
      return countIsExact;
   }

   @ProtoField(5)
   MarshallableArray<?> getKeys() {
      return MarshallableArray.create(keys);
   }

   @ProtoField(6)
   MarshallableArray<?> getProjections() {
      return MarshallableArray.create(projections);
   }
}
