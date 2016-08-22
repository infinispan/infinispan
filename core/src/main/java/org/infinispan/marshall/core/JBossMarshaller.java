package org.infinispan.marshall.core;

import java.io.IOException;

import org.infinispan.IllegalLifecycleStateException;
import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.marshall.jboss.AbstractJBossMarshaller;
import org.infinispan.commons.marshall.jboss.DefaultContextClassResolver;
import org.infinispan.commons.marshall.jboss.SerializeWithExtFactory;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.jboss.marshalling.ClassResolver;
import org.jboss.marshalling.Externalize;
import org.jboss.marshalling.ObjectTable;
import org.jboss.marshalling.Unmarshaller;

/**
 * A JBoss Marshalling based marshaller that is oriented at internal, embedded,
 * Infinispan usage. It uses of a custom object table for Infinispan based
 * Externalizer instances that are either internal or user defined.
 * <p />
 * The reason why this is implemented specially in Infinispan rather than resorting to Java serialization or even the
 * more efficient JBoss serialization is that a lot of efficiency can be gained when a majority of the serialization
 * that occurs has to do with a small set of known types such as {@link org.infinispan.transaction.xa.GlobalTransaction} or
 * {@link org.infinispan.commands.ReplicableCommand}, and class type information can be replaced with simple magic
 * numbers.
 * <p/>
 * Unknown types (typically user data) falls back to Java serialization.
 *
 * @author Galder Zamarreño
 * @author Sanne Grinovero
 * @since 4.0
 */
public class JBossMarshaller extends AbstractJBossMarshaller implements StreamingMarshaller {

//   final ExternalizerTable externalizerTable;
//   ExternalizerTableProxy proxy;
   final GlobalConfiguration globalCfg;

   public JBossMarshaller() {
//      this.externalizerTable = null;
      this.globalCfg = null;
   }

   public JBossMarshaller(ExternalizerTable externalizerTable,
         GlobalConfiguration globalCfg) {
//      this.externalizerTable = externalizerTable;
      this.globalCfg = globalCfg;
   }

   @Override
   public void start() {
      super.start();

      baseCfg.setClassExternalizerFactory(new SerializeWithExtFactory());

//      proxy = new ExternalizerTableProxy(externalizerTable);
//      baseCfg.setObjectTable(proxy);

      ClassResolver classResolver = globalCfg.serialization().classResolver();
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
      // Remove the ExternalizerTable reference from all the threads.
      // Don't need to re-populate the proxy on start, as the component is volatile.
      //proxy.clear();
   }

   @Override
   public boolean isMarshallableCandidate(Object o) {
      return super.isMarshallableCandidate(o)
            //|| externalizerTable.isMarshallableCandidate(o)
            || o.getClass().getAnnotation(SerializeWith.class) != null
            || o.getClass().getAnnotation(Externalize.class) != null;
   }

//   /**
//    * Proxy for {@code ExternalizerTable}, used to remove the references to the real {@code ExternalizerTable}
//    * from all the threads that have a {@code PerThreadInstanceHolder}.
//    *
//    * This is useful because {@code ExternalizerTable} can keep lots of other objects alive through its
//    * {@code GlobalComponentRegistry} and {@code RemoteCommandsFactory} fields.
//    */
//   private static final class ExternalizerTableProxy implements ObjectTable {
//      private ExternalizerTable externalizerTable;
//
//      public ExternalizerTableProxy(ExternalizerTable externalizerTable) {
//         this.externalizerTable = externalizerTable;
//         log.tracef("Initialized proxy %s with table %s", this, externalizerTable);
//      }
//
//      public void clear() {
//         externalizerTable = null;
//         log.tracef("Cleared proxy %s", this);
//      }
//
//      @Override
//      public Writer getObjectWriter(Object o) throws IOException {
//         return getExternalizerTable().getObjectWriter(o);
//      }
//
//      @Override
//      public Object readObject(Unmarshaller input) throws IOException, ClassNotFoundException {
//         return getExternalizerTable().readObject(input);
//      }
//
//      private ExternalizerTable getExternalizerTable() {
//         ExternalizerTable table = this.externalizerTable;
//         if (table == null) {
//            throw new IllegalLifecycleStateException("Cache marshaller has been stopped");
//         }
//         return table;
//      }
//   }
}
