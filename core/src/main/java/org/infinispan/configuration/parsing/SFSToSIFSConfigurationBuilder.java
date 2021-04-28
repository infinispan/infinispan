package org.infinispan.configuration.parsing;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.persistence.sifs.configuration.DataConfiguration;
import org.infinispan.persistence.sifs.configuration.IndexConfiguration;
import org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfiguration;
import org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfigurationBuilder;
import org.infinispan.util.logging.Log;

public class SFSToSIFSConfigurationBuilder extends SoftIndexFileStoreConfigurationBuilder {
   public SFSToSIFSConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder);
   }

   @Override
   public SoftIndexFileStoreConfiguration create() {
      return new SFSToSIFSConfiguration(attributes.protect(), async.create(), index.create(), data.create());
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
      // Without global state we need to make sure the index location is set to something based on the data location.
      // The user may have configured only the path for the SingleFileStore
      if (!globalConfig.globalState().enabled()) {
         Attribute<String> indexLocation = index.attributes().attribute(IndexConfiguration.INDEX_LOCATION);
         if (!indexLocation.isModified()) {
            Attribute<String> dataLocation = data.attributes().attribute(DataConfiguration.DATA_LOCATION);
            if (dataLocation.isModified()) {
               String indexLocationString = dataLocation.get() + "-index";
               Log.CONFIG.debugf("Setting index location for migrated FileStore to %s", indexLocationString);
               indexLocation.set(indexLocationString);
            }
         }
      }
      super.validate(globalConfig);
   }
}
