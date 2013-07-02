package org.infinispan.container.versioning;

import net.jcip.annotations.Immutable;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.marshall.core.Ids;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

/**
 * A simple versioning scheme that is cluster-aware
 *
 * @author Manik Surtani
 * @since 5.1
 */
@Immutable
public class SimpleClusteredVersion implements IncrementableEntryVersion {

   /**
    * The cache topology id in which it was first created.
    */
   private final int topologyId;

   final long version;

   public SimpleClusteredVersion(int topologyId, long version) {
      this.version = version;
      this.topologyId = topologyId;
   }

   @Override
   public InequalVersionComparisonResult compareTo(EntryVersion other) {
      if (other instanceof SimpleClusteredVersion) {
         SimpleClusteredVersion otherVersion = (SimpleClusteredVersion) other;

         if (topologyId > otherVersion.topologyId)
            return InequalVersionComparisonResult.AFTER;
         if (topologyId < otherVersion.topologyId)
            return InequalVersionComparisonResult.BEFORE;

         if (version > otherVersion.version)
            return InequalVersionComparisonResult.AFTER;
         if (version < otherVersion.version)
            return InequalVersionComparisonResult.BEFORE;

         return InequalVersionComparisonResult.EQUAL;
      } else {
         throw new IllegalArgumentException("I only know how to deal with SimpleClusteredVersions, not " + other.getClass().getName());
      }
   }

   @Override
   public String toString() {
      return "SimpleClusteredVersion{" +
            "topologyId=" + topologyId +
            ", version=" + version +
            '}';
   }

   public static class Externalizer extends AbstractExternalizer<SimpleClusteredVersion> {

      @Override
      public void writeObject(ObjectOutput output, SimpleClusteredVersion ch) throws IOException {
         output.writeInt(ch.topologyId);
         output.writeLong(ch.version);
      }

      @Override
      @SuppressWarnings("unchecked")
      public SimpleClusteredVersion readObject(ObjectInput unmarshaller) throws IOException, ClassNotFoundException {
         int topologyId = unmarshaller.readInt();
         long version = unmarshaller.readLong();
         return new SimpleClusteredVersion(topologyId, version);
      }

      @Override
      public Integer getId() {
         return Ids.SIMPLE_CLUSTERED_VERSION;
      }

      @Override
      public Set<Class<? extends SimpleClusteredVersion>> getTypeClasses() {
         return Collections.<Class<? extends SimpleClusteredVersion>>singleton(SimpleClusteredVersion.class);
      }
   }
}
