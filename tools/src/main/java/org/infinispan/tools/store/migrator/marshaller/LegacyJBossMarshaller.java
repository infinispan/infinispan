package org.infinispan.tools.store.migrator.marshaller;

import java.util.Map;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.marshall.jboss.AbstractJBossMarshaller;
import org.infinispan.commons.marshall.jboss.DefaultContextClassResolver;
import org.infinispan.commons.marshall.jboss.SerializeWithExtFactory;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;

/**
 * A JBossMarshaller implementation used exclusively for reading byte arrays marshalled by Infinispan 8.
 */
class LegacyJBossMarshaller extends AbstractJBossMarshaller implements StreamingMarshaller {
   LegacyJBossMarshaller(StreamingMarshaller parent, Map<Integer, ? extends AdvancedExternalizer<?>> externalizerMap) {
      baseCfg.setClassExternalizerFactory(new SerializeWithExtFactory());
      baseCfg.setObjectTable(new ExternalizerTable(parent, externalizerMap));
      baseCfg.setClassResolver(new DefaultContextClassResolver(GlobalConfigurationBuilder.class.getClassLoader()));
   }
}
