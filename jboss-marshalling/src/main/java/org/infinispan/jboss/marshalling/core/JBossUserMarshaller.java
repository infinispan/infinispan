package org.infinispan.jboss.marshalling.core;

import static org.infinispan.marshall.core.GlobalMarshaller.ID_EXTERNAL;
import static org.infinispan.marshall.core.GlobalMarshaller.writeExternalClean;

import java.io.IOException;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.marshall.core.impl.ClassToExternalizerMap;
import org.infinispan.marshall.core.impl.ExternalExternalizers;
import org.jboss.marshalling.ObjectTable;
import org.jboss.marshalling.Unmarshaller;

/**
 * An extension of the {@link JBossMarshaller} that loads user defined {@link org.infinispan.commons.marshall.Externalizer}
 * implementations. This class can be removed if/when we no longer support a jboss-marshalling based user marshaller.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
@SuppressWarnings("unused")
public class JBossUserMarshaller extends JBossMarshaller {

   public static final int USER_EXT_ID_MIN = AdvancedExternalizer.USER_EXT_ID_MIN;

   private ClassToExternalizerMap externalExts;

   public JBossUserMarshaller(GlobalComponentRegistry gcr) {
      this.globalCfg = gcr.getGlobalConfiguration();
      // Only load the externalizers outside of the ISPN reserved range, this ensures that we don't accidentally persist internal types
      this.externalExts = ExternalExternalizers.load(globalCfg, USER_EXT_ID_MIN, Integer.MAX_VALUE);
      this.objectTable = new UserExternalizerObjectTable();
   }

   @Override
   public boolean isMarshallable(Object o) throws Exception {
      return (externalExts.get(o.getClass()) != null || super.isMarshallable(o));
   }

   /**
    * A {@link org.jboss.marshalling.ObjectTable} implementation that creates {@link org.jboss.marshalling.ObjectTable.Writer}
    * based upon a users configured {@link org.infinispan.commons.marshall.Externalizer} implementations.
    */
   class UserExternalizerObjectTable implements ObjectTable {

      final ClassToExternalizerMap.IdToExternalizerMap reverseExts = externalExts.reverseMap();

      @Override
      public ObjectTable.Writer getObjectWriter(Object object) {
         Class clazz = object.getClass();
         AdvancedExternalizer ext = externalExts.get(clazz);
         return ext != null ? (out, obj) -> writeExternalClean(obj, ext, out) : null;
      }

      @Override
      public Object readObject(Unmarshaller unmarshaller) throws IOException, ClassNotFoundException {
         int type = unmarshaller.readUnsignedByte();
         if (type != ID_EXTERNAL)
            throw new IllegalStateException(String.format("Expected type %s but received %s", ID_EXTERNAL, type));

         int externalizerId = unmarshaller.readInt();
         return reverseExts.get(externalizerId).readObject(unmarshaller);
      }
   }
}
