package org.infinispan.marshall.core;

import java.io.IOException;
import java.io.ObjectOutput;
import java.util.function.BiConsumer;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.marshall.jboss.AbstractJBossMarshaller;
import org.infinispan.commons.marshall.jboss.SerializeWithExtFactory;
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

   private final GlobalMarshaller marshaller;
   private final ClassResolver classResolver;


   public JBossMarshaller() {
      this.classResolver = null;
      this.marshaller = null;
   }

   public JBossMarshaller(GlobalMarshaller marshaller, ClassResolver classResolver) {
      this.marshaller = marshaller;
      this.classResolver = classResolver;
   }

   @Override
   public void start() {
      super.start();

      baseCfg.setClassExternalizerFactory(new SerializeWithExtFactory());

      baseCfg.setObjectTable(new ObjectTable() {
         @Override
         public Writer getObjectWriter(Object object) {
            BiConsumer<ObjectOutput, Object> writer = marshaller.findWriter(object);
            return writer != null ? writer::accept : null;
         }

         @Override
         public Object readObject(Unmarshaller unmarshaller) throws IOException, ClassNotFoundException {
            AdvancedExternalizer<Object> ext = marshaller.findExternalizerIn(unmarshaller);
            return ext.readObject(unmarshaller);
         }
      });
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
