package org.infinispan.marshall.persistence.impl;

import java.lang.invoke.SerializedLambda;

import org.infinispan.commons.configuration.ClassWhiteList;

/**
 * A white list for Infinispan internal java classes which do not have a {@link org.infinispan.commons.marshall.Externalizer}
 * instance defined.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
class UserMarshallerWhiteList {
   static void addInternalClassesToWhiteList(ClassWhiteList list) {
      list.addClasses(Number.class, Object.class, Object[].class, SerializedLambda.class);
   }
}
