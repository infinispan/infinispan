package org.infinispan.affinity.impl;

import java.util.Random;

import org.infinispan.affinity.KeyGenerator;

/**
 * Key provider that relies on {@link java.util.Random}'s distribution to generate keys.
 * It doesn't offer any guarantee that the keys are unique.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class RndKeyGenerator implements KeyGenerator<Object> {

   public static final Random rnd = new Random();

   @Override
   public Object getKey() {
      return rnd.nextLong();
   }
}
