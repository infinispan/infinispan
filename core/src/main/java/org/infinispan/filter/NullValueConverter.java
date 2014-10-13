package org.infinispan.filter;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.Metadata;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Set;

/**
 * Converter that returns null for the value.  This can be useful when retrieving all the keys or counting
 * them only.
 *
 * @author wburns
 * @since 7.0
 */
public class NullValueConverter implements Converter<Object, Object, Void>, Serializable {

   private NullValueConverter() { }

   private static class StaticHolder {
      private static final NullValueConverter INSTANCE = new NullValueConverter();
   }

   public static NullValueConverter getInstance() {
      return StaticHolder.INSTANCE;
   }

   @Override
   public Void convert(Object key, Object value, Metadata metadata) {
      return null;
   }

   public static class Externalizer extends AbstractExternalizer<NullValueConverter> {
      @Override
      public Set<Class<? extends NullValueConverter>> getTypeClasses() {
         return Util.<Class<? extends NullValueConverter>>asSet(NullValueConverter.class);
      }

      @Override
      public void writeObject(ObjectOutput output, NullValueConverter object) throws IOException {
      }

      @Override
      public NullValueConverter readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return NullValueConverter.getInstance();
      }

      @Override
      public Integer getId() {
         return Ids.NULL_VALUE_CONVERTER;
      }
   }
}
