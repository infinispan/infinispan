package org.infinispan.marshall.core;

import static org.infinispan.marshall.core.GlobalMarshaller.ID_EXTERNAL;
import static org.infinispan.marshall.core.GlobalMarshaller.writeExternalClean;

import java.io.IOException;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.factories.GlobalComponentRegistry;
import org.jboss.marshalling.ObjectTable;
import org.jboss.marshalling.Unmarshaller;

/**
 * An extension of the {@link JBossMarshaller} that utilises {@link InternalExternalizers} as a blacklist.
 * This class can be removed once the {@link GlobalMarshaller} is no longer based upon jboss-marshalling.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
public class JBossUserMarshaller extends JBossMarshaller {

   private final ClassToExternalizerMap externalExts;
   private final ClassToExternalizerMap internalExts;

   public JBossUserMarshaller(GlobalComponentRegistry gcr) {
      this.globalCfg = gcr.getGlobalConfiguration();
      this.externalExts = ExternalExternalizers.load(globalCfg);
      this.internalExts = InternalExternalizers.load(gcr, null);
      this.objectTable = new UserExternalizerObjectTable();
   }

   @Override
   public boolean isMarshallable(Object o) throws Exception {
      // If a class have an internal externalizer, then it should not be marshallable by the user marshaller
      return internalExts.get(o.getClass()) == null && (externalExts.get(o.getClass()) != null || super.isMarshallable(o));
   }

   /**
    * A {@link org.jboss.marshalling.ObjectTable} implementation that creates {@link org.jboss.marshalling.ObjectTable.Writer}
    * based upon a users configured {@link org.infinispan.marshall.core.MarshalledEntryImpl.Externalizer} implementation.
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
