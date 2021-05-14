package org.infinispan.commons.util;

import java.io.Serializable;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * A placeholder for {@literal null} in a cache, because caches do not allow {@literal null} values.
 *
 * @author Dan Berindei
 * @since 13.0
 */
@ProtoTypeId(ProtoStreamTypeIds.NULL_VALUE)
public final class NullValue implements Serializable {

   private static final long serialVersionUID = 2860710533033240004L;

   public static final NullValue NULL = new NullValue();

   private NullValue() {
   }

   private Object readResolve() {
      return NULL;
   }

   @ProtoFactory
   public static NullValue getInstance() {
      return NULL;
   }

   // Object implementations of equals() and hashCode() work fine
}
