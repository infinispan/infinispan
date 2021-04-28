package org.infinispan.configuration.parsing;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.persistence.sifs.configuration.DataConfiguration;
import org.infinispan.persistence.sifs.configuration.IndexConfiguration;
import org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfiguration;

@BuiltBy(SFSToSIFSConfigurationBuilder.class)
@ConfigurationFor(SFSToSIFSStore.class)
public class SFSToSIFSConfiguration extends SoftIndexFileStoreConfiguration {
   public SFSToSIFSConfiguration(AttributeSet attributes, AsyncStoreConfiguration async,
         IndexConfiguration indexConfiguration, DataConfiguration dataConfiguration) {
      super(attributes, async, indexConfiguration, dataConfiguration);
   }
}
