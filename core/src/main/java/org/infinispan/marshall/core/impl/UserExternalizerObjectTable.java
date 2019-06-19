package org.infinispan.marshall.core.impl;

import static org.infinispan.marshall.core.impl.GlobalMarshaller.ID_EXTERNAL;
import static org.infinispan.marshall.core.impl.GlobalMarshaller.writeExternalClean;

import java.io.IOException;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.jboss.marshalling.ObjectTable;
import org.jboss.marshalling.Unmarshaller;

/**
 * A {@link org.jboss.marshalling.ObjectTable} implementation that creates {@link org.jboss.marshalling.ObjectTable.Writer}
 * based upon a users configured {@link org.infinispan.commons.marshall.Externalizer} implementations.
 */
public class UserExternalizerObjectTable implements ObjectTable {

   final ClassToExternalizerMap externalExts;
   final ClassToExternalizerMap.IdToExternalizerMap reverseExts;

   public UserExternalizerObjectTable(GlobalConfiguration globalCfg, int minId, int maxId) {
      this.externalExts = ExternalExternalizers.load(globalCfg, minId, maxId);
      this.reverseExts = externalExts.reverseMap();
   }

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

   public boolean isExternalizerAvailable(Object o) {
      return externalExts.get(o.getClass()) != null;
   }

}
