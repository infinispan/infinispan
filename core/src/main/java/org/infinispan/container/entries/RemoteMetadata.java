package org.infinispan.container.entries;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.InvocationRecord;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.commons.util.Util;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.SimpleClusteredVersion;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.remoting.transport.Address;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

/**
 * This is a metadata type used by scattered cache during state transfer. The address points to node which has last
 * known version of given entry: During key transfer RemoteMetadata is created and overwritten if another response
 * with higher version comes. During value transfer the address is already final and we request the value + metadata
 * only from this node.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class RemoteMetadata implements InternalMetadata {
   private final Address address;
   private final int topologyId;
   private final long version;

   public RemoteMetadata(Address address, EntryVersion version) {
      this.address = address;
      SimpleClusteredVersion scv = (SimpleClusteredVersion) version;
      this.topologyId = scv.topologyId;
      this.version = scv.version;
   }

   private RemoteMetadata(Address address, int topologyId, long version) {
      this.address = address;
      this.topologyId = topologyId;
      this.version = version;
   }

   public Address getAddress() {
      return address;
   }

   @Override
   public long created() {
      return -1;
   }

   @Override
   public long lastUsed() {
      return -1;
   }

   @Override
   public boolean isExpired(long now) {
      return false;
   }

   @Override
   public long expiryTime() {
      return -1;
   }

   @Override
   public long lifespan() {
      return -1;
   }

   @Override
   public long maxIdle() {
      return -1;
   }

   @Override
   public EntryVersion version() {
      return new SimpleClusteredVersion(topologyId, version);
   }

   @Override
   public InvocationRecord lastInvocation() {
      return null;
   }

   @Override
   public InvocationRecord invocation(CommandInvocationId id) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Builder builder() {
      throw new UnsupportedOperationException();
   }

   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder("RemoteMetadata{");
      sb.append("address=").append(address);
      sb.append(", version=").append(version);
      sb.append('}');
      return sb.toString();
   }

   public static class Externalizer implements AdvancedExternalizer<RemoteMetadata> {
      @Override
      public Set<Class<? extends RemoteMetadata>> getTypeClasses() {
         return Util.asSet(RemoteMetadata.class);
      }

      @Override
      public Integer getId() {
         return Ids.METADATA_REMOTE;
      }

      @Override
      public void writeObject(ObjectOutput output, RemoteMetadata entry) throws IOException {
         output.writeObject(entry.getAddress());
         output.writeInt(entry.topologyId);
         output.writeLong(entry.version);
      }

      @Override
      public RemoteMetadata readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Address address = (Address) input.readObject();
         int topologyId = input.readInt();
         long version = input.readLong();
         return new RemoteMetadata(address, topologyId, version);
      }
   }
}
