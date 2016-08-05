package org.infinispan.container.entries;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.remoting.transport.Address;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class RemoteMetadata implements InternalMetadata {
   private final Address address;
   private final long version;

   public RemoteMetadata(Address address, long version) {
      this.address = address;
      this.version = version;
   }

   public Address getAddress() {
      return address;
   }

   public long getVersion() {
      return version;
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
      return false;  // TODO: Customise this generated block
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
      return new NumericVersion(version);
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
         output.writeLong(entry.getVersion());
      }

      @Override
      public RemoteMetadata readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Address address = (Address) input.readObject();
         long version = input.readLong();
         return new RemoteMetadata(address, version);
      }
   }
}
