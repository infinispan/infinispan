package org.infinispan.marshall.core;

import java.io.IOException;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.marshall.jboss.AbstractJBossMarshaller;
import org.infinispan.commons.marshall.jboss.DefaultContextClassResolver;
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

   final ExternalizerTable externalizerTable;
   ExternalizerTableProxy proxy;
   final GlobalConfiguration globalCfg;
   final Configuration cfg;
   final InvocationContextContainer icc;

   public JBossMarshaller() {
      this.cfg = null;
      this.externalizerTable = null;
      this.globalCfg = null;
      this.icc = null;
   }

   public JBossMarshaller(ExternalizerTable externalizerTable, Configuration cfg,
         InvocationContextContainer icc, GlobalConfiguration globalCfg) {
      this.externalizerTable = externalizerTable;
      this.globalCfg = globalCfg;
      this.cfg = cfg;
      this.icc = icc;
   }

   @Override
   public void start() {
      super.start();

      baseCfg.setClassExternalizerFactory(new SerializeWithExtFactory());
      baseCfg.setObjectTable(externalizerTable);

      proxy = new ExternalizerTableProxy(externalizerTable);
      baseCfg.setObjectTable(proxy);

      ClassResolver classResolver = globalCfg.serialization().classResolver();
      if (classResolver == null) {
         // Override the class resolver with one that can detect injected
         // classloaders via AdvancedCache.with(ClassLoader) calls.
         ClassLoader cl = cfg == null ? globalCfg.classLoader() : cfg.classLoader();
         classResolver = new EmbeddedContextClassResolver(cl, icc);
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
      proxy.clear();
   }

   @Override
   public boolean isMarshallableCandidate(Object o) {
      return super.isMarshallableCandidate(o)
            || externalizerTable.isMarshallableCandidate(o)
            || o.getClass().getAnnotation(SerializeWith.class) != null
            || o.getClass().getAnnotation(Externalize.class) != null
            || o.getClass().getAnnotation(org.infinispan.marshall.SerializeWith.class) != null; // Legacy annotation
   }

   /**
    * An embedded context class resolver that is able to retrieve a class
    * loader from the embedded Infinispan call context. This might happen when
    * {@link org.infinispan.AdvancedCache#with(ClassLoader)} is used.
    */
   public static final class EmbeddedContextClassResolver extends DefaultContextClassResolver {

      private final InvocationContextContainer icc;

      public EmbeddedContextClassResolver(ClassLoader defaultClassLoader, InvocationContextContainer icc) {
         super(defaultClassLoader);
         this.icc = icc;
      }

      @Override
      protected ClassLoader getClassLoader() {
         if (icc != null) {
            InvocationContext ctx = icc.getInvocationContext(true);
            if (ctx != null) {
               ClassLoader cl = ctx.getClassLoader();
               if (cl != null) return cl;
            }
         }
         return super.getClassLoader();
      }
   }

   /**
    * Proxy for {@code ExternalizerTable}, used to remove the references to the real {@code ExternalizerTable}
    * from all the threads that have a {@code PerThreadInstanceHolder}.
    *
    * This is useful because {@code ExternalizerTable} can keep lots of other objects alive through its
    * {@code GlobalComponentRegistry} and {@code RemoteCommandsFactory} fields.
    */
   private static final class ExternalizerTableProxy implements ObjectTable {
      private ExternalizerTable externalizerTable;

      public ExternalizerTableProxy(ExternalizerTable externalizerTable) {
         this.externalizerTable = externalizerTable;
         log.tracef("Initialized proxy %s with table %s", this, externalizerTable);
      }

      public void clear() {
         externalizerTable = null;
         log.tracef("Cleared proxy %s", this);
      }

      @Override
      public Writer getObjectWriter(Object o) throws IOException {
         return externalizerTable.getObjectWriter(o);
      }

      @Override
      public Object readObject(Unmarshaller input) throws IOException, ClassNotFoundException {
         return externalizerTable.readObject(input);
      }
   }
}
