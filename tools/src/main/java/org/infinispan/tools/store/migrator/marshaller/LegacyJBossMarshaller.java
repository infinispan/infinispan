package org.infinispan.tools.store.migrator.marshaller;

import org.infinispan.jboss.marshalling.commons.StreamingMarshaller;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.jboss.marshalling.commons.AbstractJBossMarshaller;
import org.infinispan.jboss.marshalling.commons.DefaultContextClassResolver;
import org.jboss.marshalling.AnnotationClassExternalizerFactory;
import org.jboss.marshalling.ObjectTable;

public class LegacyJBossMarshaller extends AbstractJBossMarshaller implements StreamingMarshaller {
   public LegacyJBossMarshaller(ObjectTable objectTable) {
      baseCfg.setClassExternalizerFactory(new AnnotationClassExternalizerFactory());
      baseCfg.setObjectTable(objectTable);
      baseCfg.setClassResolver(new DefaultContextClassResolver(GlobalConfigurationBuilder.class.getClassLoader()));
   }
}
