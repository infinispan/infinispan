package org.infinispan.persistence.remote.upgrade;

import org.infinispan.commons.configuration.ClassWhiteList;
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

   public MigrationMarshaller(ClassWhiteList classWhiteList) {
      super();
      baseCfg.setClassResolver(new CheckedClassResolver(classWhiteList, this.getClass().getClassLoader()));
   }

}
