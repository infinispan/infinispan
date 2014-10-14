package org.infinispan.cli.interpreter.codec;

/**
 *
 * NoneCodec. This codec leaves keys/values as is without applying any transformation
 * It is the default codec.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class NoneCodec extends AbstractCodec {

   @Override
   public String getName() {
      return "none";
   }

   @Override
   public Object encodeKey(Object key) {
      return key;
   }

   @Override
   public Object encodeValue(Object value) {
      return value;
   }

   @Override
   public Object decodeKey(Object key) {
      return key;
   }

   @Override
   public Object decodeValue(Object value) {
      return value;
   }

}
