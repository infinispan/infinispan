package org.infinispan.tools.store.migrator.marshaller;

import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.jboss.marshalling.commons.AbstractJBossMarshaller;
import org.infinispan.jboss.marshalling.commons.DefaultContextClassResolver;
import org.infinispan.jboss.marshalling.commons.SerializeWithExtFactory;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.jboss.marshalling.ObjectTable;

public class LegacyJBossMarshaller extends AbstractJBossMarshaller implements StreamingMarshaller {
   public LegacyJBossMarshaller(ObjectTable objectTable) {
      baseCfg.setClassExternalizerFactory(new SerializeWithExtFactory());
      baseCfg.setObjectTable(objectTable);
      baseCfg.setClassResolver(new DefaultContextClassResolver(GlobalConfigurationBuilder.class.getClassLoader()));
   }
}
