package org.infinispan.persistence.remote.upgrade;

import org.infinispan.commons.marshall.jboss.AbstractJBossMarshaller;
import org.infinispan.commons.marshall.jboss.DefaultContextClassResolver;

/**
 * MigrationMarshaller. Uses the Remote Store classloader so that appropriate
 * backwards-compatibility classes are loaded.
 *
 * @author Tristan Tarrant
 * @since 6.0
 */
public class MigrationMarshaller extends AbstractJBossMarshaller {

   public MigrationMarshaller() {
      super();
      baseCfg.setClassResolver(
            new DefaultContextClassResolver(this.getClass().getClassLoader()));
   }

}

