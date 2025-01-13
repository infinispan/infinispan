package org.infinispan.tools.store.migrator.marshaller.infinispan8;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.util.Map;

import org.infinispan.tools.store.migrator.marshaller.common.AdvancedExternalizer;
import org.infinispan.tools.store.migrator.marshaller.LegacyJBossMarshaller;
import org.infinispan.tools.store.migrator.marshaller.common.AbstractUnsupportedStreamingMarshaller;

/**
 * LegacyVersionAwareMarshaller that is used to read bytes marshalled using Infinispan 8.x. This is useful for providing
 * a migration path from 8.x stores.
 */
public class Infinispan8Marshaller extends AbstractUnsupportedStreamingMarshaller {
   private final LegacyJBossMarshaller external;

   public Infinispan8Marshaller(Map<Integer, ? extends AdvancedExternalizer> userExts) {
      this.external = new LegacyJBossMarshaller(new ExternalizerTable(this, userExts));
   }

   @Override
   public Object objectFromByteBuffer(byte[] bytes, int offset, int len) throws IOException, ClassNotFoundException {
      ByteArrayInputStream is = new ByteArrayInputStream(bytes, offset, len);
      ObjectInput in = startObjectInput(is, false);
      Object o;
      try {
         o = external.objectFromObjectStream(in);
      } finally {
         finishObjectInput(in);
      }
      return o;
   }

   @Override
   public ObjectInput startObjectInput(InputStream is, boolean isReentrant) throws IOException {
      ObjectInput in = external.startObjectInput(is, isReentrant);
      try {
         in.readShort();
      } catch (Exception e) {
         finishObjectInput(in);
         throw new IOException("Unable to read version id from first two bytes of stream: " + e.getMessage());
      }
      return in;
   }

   @Override
   public void finishObjectInput(ObjectInput oi) {
      external.finishObjectInput(oi);
   }
}
