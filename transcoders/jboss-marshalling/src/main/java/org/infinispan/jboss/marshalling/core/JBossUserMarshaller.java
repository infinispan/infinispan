package org.infinispan.jboss.marshalling.core;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.jboss.marshalling.commons.AbstractJBossMarshaller;
import org.infinispan.jboss.marshalling.commons.DefaultContextClassResolver;
import org.jboss.marshalling.AnnotationClassExternalizerFactory;
import org.jboss.marshalling.ClassResolver;
import org.jboss.marshalling.Externalize;
import org.jboss.marshalling.ObjectTable;

/**
 * Legacy marshaller implementation that previously loaded Externalizer instances. Users should migrate to
 * {@link org.infinispan.jboss.marshalling.commons.GenericJBossMarshaller}.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
@Deprecated(since = "16.0", forRemoval = true)
@SuppressWarnings("unused")
public class JBossUserMarshaller extends AbstractJBossMarshaller {

   GlobalConfiguration globalCfg;
   ObjectTable objectTable;
   ClassResolver classResolver;

   public JBossUserMarshaller() {
      this(null);
   }

   public JBossUserMarshaller(ClassResolver classResolver) {
      this.classResolver = classResolver;
   }

   public void initialize(GlobalComponentRegistry gcr) {
      this.globalCfg = gcr.getGlobalConfiguration();
   }

   @Override
   public void start() {
      super.start();
      baseCfg.setClassExternalizerFactory(new AnnotationClassExternalizerFactory());
      baseCfg.setObjectTable(objectTable);

      if (classResolver == null) {
         // Override the class resolver with one that can detect injected
         // classloaders via AdvancedCache.with(ClassLoader) calls.
         ClassLoader cl = globalCfg.classLoader();
         classResolver = new DefaultContextClassResolver(cl);
      }
      baseCfg.setClassResolver(classResolver);
   }

   @Override
   public void stop() {
      super.stop();
      // Just in case, to avoid leaking class resolver which references classloader
      baseCfg.setClassResolver(null);
   }

   @Override
   public boolean isMarshallableCandidate(Object o) {
      return super.isMarshallableCandidate(o) || o.getClass().getAnnotation(Externalize.class) != null;
   }
}
