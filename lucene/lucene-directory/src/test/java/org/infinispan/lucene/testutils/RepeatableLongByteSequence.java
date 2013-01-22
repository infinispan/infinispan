/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.lucene.testutils;

import org.testng.annotations.Test;

/**
 * RepeatableLongByteSequence is a testing utility to get a source of bytes.
 * Use nextByte() to produce them.
 * The generated sequence is similar to a random generated sequence, but will always generate
 * the same sequence and avoid immediate repetitions of bytes and
 * close repetitive patterns (they might occur in large scale).
 * 
 * After having written such a stream from one
 * instance, create a second instance to assert equality of contents (see test)
 * as the source is not random and will generate the same sequence.
 * 
 * @author Sanne Grinovero
 * @since 4.0
 */
public class RepeatableLongByteSequence {

   private byte lastUsedValue = -1;
   private byte currentMax = (byte) 1;
   private byte currentMin = (byte) -1;
   private boolean rising = true;

   public byte nextByte() {
      byte next;
      if (rising) {
         next = ++lastUsedValue;
         if (next == currentMax) {
            rising = false;
            currentMax++; // overflow might occur, not bad for our purposes.
         }
      } else {
         next = --lastUsedValue;
         if (next == currentMin) {
            rising = true;
            currentMin--; // as above: overflow allowed
         }
      }
      return next;
   }

   /**
    * @param buffer is going to be modified: a new series of bytes is going to be written into
    */
   public void nextBytes(byte[] buffer) {
      for(int i=0; i < buffer.length; i++) {
         buffer[i] = nextByte();
      }
   }
   
   public void reset() {
      lastUsedValue = -1;
      currentMax = (byte) 1;
      currentMin = (byte) -1;
      rising = true;
   }

}
