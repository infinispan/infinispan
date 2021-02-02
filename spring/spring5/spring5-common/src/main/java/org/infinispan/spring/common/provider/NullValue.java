package org.infinispan.spring.common.provider;

import java.io.Serializable;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.springframework.cache.Cache.ValueWrapper;

/**
 * A placeholder value for storing {@literal null} in a cache.
 *
 * @author <a href="mailto:olaf.bergner@gmx.de">Olaf Bergner</a>
 * @since 5.3
 */
@ProtoTypeId(ProtoStreamTypeIds.SPRING_NULL_VALUE)
public final class NullValue implements ValueWrapper, Serializable {

   /** The serialVersionUID */
   private static final long serialVersionUID = 6104836300055942197L;

   public static final NullValue NULL = new NullValue();

   /**
    * Always returns {@literal null}.
    *
    * @return {@literal null}
    * @see ValueWrapper#get()
    */
   @Override
   public Object get() {
      return null;
   }

   private NullValue() {
   }

   private Object readResolve() {
      return NULL;
   }

   @ProtoFactory
   public static NullValue getInstance() {
      return NULL;
   }

   public boolean equals(Object obj) {
      return this == obj || obj == null;
   }

   public int hashCode() {
      return NullValue.class.hashCode();
   }
}
