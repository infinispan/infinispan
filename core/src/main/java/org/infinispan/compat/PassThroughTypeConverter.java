package org.infinispan.compat;

import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.context.Flag;

/**
 * Type Converter that does nothing and just passes the value back as is
 *
 * @author wburns
 * @since 9.0
 */
public class PassThroughTypeConverter implements TypeConverter<Object, Object, Object, Object> {

   @Override
   public Object boxKey(Object key) {
      return key;
   }

   @Override
   public Object boxValue(Object value) {
      return value;
   }

   @Override
   public Object unboxKey(Object target) {
      return target;
   }

   @Override
   public Object unboxValue(Object target) {
      return target;
   }

   @Override
   public boolean supportsInvocation(Flag flag) {
      return false;
   }

   @Override
   public void setMarshaller(Marshaller marshaller) {
   }
}
