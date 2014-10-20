package org.infinispan.spring.provider;

import org.infinispan.commons.marshall.SerializeWith;
import org.springframework.cache.Cache.ValueWrapper;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

/**
 * A placeholder value for storing {@literal null} in a cache.
 *
 * @author <a href="mailto:olaf.bergner@gmx.de">Olaf Bergner</a>
 * @since 5.3
 */
@SerializeWith(NullValue.Externalizer.class)
final class NullValue implements ValueWrapper, Serializable {

   /** The serialVersionUID */
   private static final long serialVersionUID = 6104836300055942197L;

   static final NullValue NULL = new NullValue();

   /**
    * Always returns {@literal null}.
    *
    * @return {@literal null}
    * @see org.springframework.cache.Cache.ValueWrapper#get()
    */
   @Override
   public Object get() {
      return null;
   }

   /**
    * Create a new NullValue.
    *
    */
   private NullValue() {
      // Intentionally left blank
   }

   public static class Externalizer implements org.infinispan.commons.marshall.Externalizer<NullValue>, Serializable {

      /** The serialVersionUID */
      private static final long serialVersionUID = -6374308529927819177L;

      /**
       * @param output
       * @param object
       * @throws IOException
       * @see org.infinispan.commons.marshall.Externalizer#writeObject(java.io.ObjectOutput,
       *      java.lang.Object)
       */
      @Override
      public void writeObject(ObjectOutput output, NullValue object) throws IOException {
         // Nothing to do?
      }

      /**
       * @param input
       * @return
       * @throws IOException
       * @throws ClassNotFoundException
       * @see org.infinispan.commons.marshall.Externalizer#readObject(java.io.ObjectInput)
       */
      @Override
      public NullValue readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return NullValue.NULL;
      }
   }
}