package org.infinispan.jboss.marshalling.core;

import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.jboss.marshalling.commons.AbstractJBossMarshaller;
import org.infinispan.jboss.marshalling.commons.DefaultContextClassResolver;
import org.infinispan.jboss.marshalling.commons.SerializeWithExtFactory;
import org.jboss.marshalling.ClassResolver;
import org.jboss.marshalling.Externalize;
import org.jboss.marshalling.ObjectTable;

/**
 * A JBoss Marshalling based marshaller that is oriented at internal, embedded, Infinispan usage. It uses of a custom
 * object table for Infinispan based Externalizer instances that are either internal or user defined.
 * <p/>
 * The reason why this is implemented specially in Infinispan rather than resorting to Java serialization or even the
 * more efficient JBoss serialization is that a lot of efficiency can be gained when a majority of the serialization
 * that occurs has to do with a small set of known types such as {@link org.infinispan.transaction.xa.GlobalTransaction}
 * or {@link org.infinispan.commands.ReplicableCommand}, and class type information can be replaced with simple magic
 * numbers.
 * <p/>
 * Unknown types (typically user data) falls back to Java serialization.
 *
 * @author Galder Zamarreño
 * @author Sanne Grinovero
 * @since 4.0
 * @deprecated since 11.0. To be removed in 14.0 ISPN-11947.
 */
@Deprecated
public class JBossMarshaller extends AbstractJBossMarshaller implements StreamingMarshaller {

   GlobalConfiguration globalCfg;
   ObjectTable objectTable;
   ClassResolver classResolver;

   JBossMarshaller(ClassResolver classResolver) {
      this.classResolver = classResolver;
   }

   @Override
   public void start() {
      super.start();
      baseCfg.setClassExternalizerFactory(new SerializeWithExtFactory());
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
      return super.isMarshallableCandidate(o)
            || o.getClass().getAnnotation(SerializeWith.class) != null
            || o.getClass().getAnnotation(Externalize.class) != null;
   }
}
