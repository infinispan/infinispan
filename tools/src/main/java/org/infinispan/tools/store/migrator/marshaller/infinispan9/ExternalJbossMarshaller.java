package org.infinispan.tools.store.migrator.marshaller.infinispan9;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;

import org.infinispan.tools.store.migrator.marshaller.common.Externalizer;
import org.infinispan.jboss.marshalling.commons.ExtendedRiverUnmarshaller;
import org.infinispan.tools.store.migrator.marshaller.LegacyJBossMarshaller;
import org.infinispan.tools.store.migrator.marshaller.common.AbstractUnsupportedStreamingMarshaller;
import org.jboss.marshalling.ObjectTable;
import org.jboss.marshalling.Unmarshaller;

class ExternalJbossMarshaller extends AbstractUnsupportedStreamingMarshaller {
   private final LegacyJBossMarshaller jbossMarshaller;

   ExternalJbossMarshaller(Infinispan9Marshaller marshaller) {
      this.jbossMarshaller = new LegacyJBossMarshaller(new ObjectTable() {
         @Override
         public Writer getObjectWriter(Object object) {
            return null;
         }

         @Override
         public Object readObject(Unmarshaller unmarshaller) throws IOException, ClassNotFoundException {
            Externalizer<Object> ext = marshaller.findExternalizerIn(unmarshaller);
            return ext == null ? null : ext.readObject(unmarshaller);
         }
      });
   }

   @Override
   public Object objectFromObjectStream(ObjectInput in) throws IOException, ClassNotFoundException {
      ExtendedRiverUnmarshaller jbossIn = (ExtendedRiverUnmarshaller)
            jbossMarshaller.startObjectInput(new InputStream() {
               @Override
               public int read() throws IOException {
                  return in.read();
               }

               @Override
               public int read(byte[] b, int off, int len) throws IOException {
                  return in.read(b, off, len);
               }
            }, false);

      try {
         return jbossIn.readObject();
      } finally {
         // Rewind by skipping backwards (negatively)
         in.skipBytes(-jbossIn.getUnreadBufferedCount());
         jbossMarshaller.finishObjectInput(jbossIn);
      }
   }
}
