package org.infinispan.server.hotrod;

import java.io.IOException;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.compat.TypeConverter;
import org.infinispan.context.Flag;

/**
 * Hot Rod type converter for compatibility mode.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public class HotRodTypeConverter implements TypeConverter<Object, Object, Object, Object> {

   // Default marshaller is the one used by the Hot Rod client,
   // but can be configured for compatibility use cases
   private Marshaller marshaller;

   public HotRodTypeConverter() {
      this(null);
   }

   public HotRodTypeConverter(Marshaller marshaller) {
      if (marshaller == null) {
         this.marshaller = new GenericJBossMarshaller();
      } else {
         this.marshaller = marshaller;
      }
   }

   @Override
   public Object boxKey(Object key) {
      return unmarshall(key);
   }

   @Override
   public Object boxValue(Object value) {
      return unmarshall(value);
   }

   @Override
   public Object unboxKey(Object target) {
      return marshall(target);
   }

   @Override
   public Object unboxValue(Object target) {
      return marshall(target);
   }

   @Override
   public boolean supportsInvocation(Flag flag) {
      return flag == Flag.OPERATION_HOTROD;
   }

   @Override
   public void setMarshaller(Marshaller marshaller) {
      this.marshaller = marshaller;
   }

   private Object unmarshall(Object source) {
      if (source instanceof byte[]) {
         try {
            return marshaller.objectFromByteBuffer((byte[]) source);
         } catch (IOException | ClassNotFoundException e) {
            throw new CacheException(e);
         }
      } else {
         return source;
      }
   }

   private Object marshall(Object source) {
      if (source != null) {
         try {
            if (marshaller.isMarshallable(source)) {
               return marshaller.objectToByteBuffer(source);
            } else {
               return source;
            }
         } catch (Exception e) {
            throw new CacheException(e);
         }
      } else return null;
   }
}
