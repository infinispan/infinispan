package org.infinispan.tools.store.migrator.marshaller;

import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.marshall.jboss.AbstractJBossMarshaller;
import org.infinispan.commons.marshall.jboss.DefaultContextClassResolver;
import org.infinispan.commons.marshall.jboss.SerializeWithExtFactory;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.jboss.marshalling.ObjectTable;

public class LegacyJBossMarshaller extends AbstractJBossMarshaller implements StreamingMarshaller {
   public LegacyJBossMarshaller(ObjectTable objectTable) {
      baseCfg.setClassExternalizerFactory(new SerializeWithExtFactory());
      baseCfg.setObjectTable(objectTable);
      baseCfg.setClassResolver(new DefaultContextClassResolver(GlobalConfigurationBuilder.class.getClassLoader()));
   }
}
