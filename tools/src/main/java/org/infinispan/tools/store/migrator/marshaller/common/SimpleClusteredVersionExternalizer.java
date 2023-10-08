package org.infinispan.tools.store.migrator.marshaller.common;

import java.io.IOException;
import java.io.ObjectInput;

import org.infinispan.container.versioning.SimpleClusteredVersion;

public class SimpleClusteredVersionExternalizer extends AbstractMigratorExternalizer<SimpleClusteredVersion> {

   public SimpleClusteredVersionExternalizer() {
      super(SimpleClusteredVersion.class, Ids.SIMPLE_CLUSTERED_VERSION);
   }

   @Override
   public SimpleClusteredVersion readObject(ObjectInput unmarshaller) throws IOException, ClassNotFoundException {
      int topologyId = unmarshaller.readInt();
      long version = unmarshaller.readLong();
      return new SimpleClusteredVersion(topologyId, version);
   }
}
