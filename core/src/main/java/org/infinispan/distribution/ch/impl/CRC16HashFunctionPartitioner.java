package org.infinispan.distribution.ch.impl;

import org.infinispan.commons.hash.CRC16;
import org.infinispan.commons.hash.Hash;

/**
 * Implementation of {@link HashFunctionPartitioner} using {@link CRC16}.
 *
 * @since 15.0
 * @see HashFunctionPartitioner
 */
public class CRC16HashFunctionPartitioner extends HashFunctionPartitioner {

   @Override
   protected Hash getHash() {
      return CRC16.getInstance();
   }
}
