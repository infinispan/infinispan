package org.infinispan.marshall.core;

import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.marshall.core.impl.UserExternalizerObjectTable;

/**
 * An extension of the {@link JBossMarshaller} that loads user defined {@link org.infinispan.commons.marshall.Externalizer}
 * implementations. This class can be removed if/when we no longer support a jboss-marshalling based user marshaller.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
public class JBossUserMarshaller extends JBossMarshaller {

   public static final int USER_EXT_ID_MIN = 2500;

   public JBossUserMarshaller(GlobalComponentRegistry gcr) {
      this.globalCfg = gcr.getGlobalConfiguration();
      // Only load the externalizers outside of the ISPN reserved range, this ensures that we don't accidentally persist internal types
      this.objectTable = new UserExternalizerObjectTable(globalCfg, USER_EXT_ID_MIN, Integer.MAX_VALUE);
   }

   @Override
   public boolean isMarshallable(Object o) throws Exception {
      return ((UserExternalizerObjectTable) objectTable).isExternalizerAvailable(o) || super.isMarshallable(o);
   }
}
