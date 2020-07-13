package org.infinispan.persistence.remote.upgrade;

import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.jboss.marshalling.commons.AbstractJBossMarshaller;
import org.infinispan.jboss.marshalling.commons.CheckedClassResolver;

/**
 * MigrationMarshaller. Uses the Remote Store classloader so that appropriate
 * backwards-compatibility classes are loaded.
 *
 * @author Tristan Tarrant
 * @since 6.0
 */
public class MigrationMarshaller extends AbstractJBossMarshaller {

   public MigrationMarshaller(ClassAllowList classAllowList) {
      super();
      baseCfg.setClassResolver(new CheckedClassResolver(classAllowList, this.getClass().getClassLoader()));
   }

}
