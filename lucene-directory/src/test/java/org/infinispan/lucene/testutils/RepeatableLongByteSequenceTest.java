package org.infinispan.lucene.testutils;/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

import org.infinispan.lucene.testutils.RepeatableLongByteSequence;
import org.testng.annotations.Test;

/**
 * Test for {@link RepeatableLongByteSequence}
 *
 * @author Dan Berindei
 * @since 5.2
 */
@Test(groups = "unit", testName = "lucene.testutils.RepeatableLongByteSequenceTest")
public class RepeatableLongByteSequenceTest {
   @Test(description="To verify the RepeatableLongByteSequence meets the requirement of producing "
      + "always the same values when using the single nextByte()")
   public void verifyRepeatability() {
      RepeatableLongByteSequence src1 = new RepeatableLongByteSequence();
      RepeatableLongByteSequence src2 = new RepeatableLongByteSequence();
      for (int i = 0; i < 1000; i++) {
         assert src1.nextByte() == src2.nextByte();
      }
   }

   @Test(description="To verify the RepeatableLongByteSequence meets the requirement of producing "
         + "always the same values when using the multivalued nextBytes()")
   public void verifyEquality() {
      RepeatableLongByteSequence src1 = new RepeatableLongByteSequence();
      RepeatableLongByteSequence src2 = new RepeatableLongByteSequence();
      final int arrayLength = 10;
      byte[] b = new byte[arrayLength];
      for (int i = 0; i < 1000; i++) {
         if((i % arrayLength) == 0) {
            src1.nextBytes(b);
         }
         assert b[i % arrayLength] == src2.nextByte();
      }
   }
}
