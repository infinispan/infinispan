package org.infinispan.commons.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collection;

import org.junit.jupiter.api.Test;


/**
 * @author Tristan Tarrant
 * @since 9.0
 */
public class ServiceFinderTest {

   @Test
   public void testDuplicateServiceFinder() {
      ClassLoader mainClassLoader = this.getClass().getClassLoader();
      ClassLoader otherClassLoader = new ClonedClassLoader(mainClassLoader);
      Collection<SampleSPI> spis = ServiceFinder.load(SampleSPI.class, mainClassLoader, otherClassLoader);
      assertEquals(1, spis.size());
   }

   public static class ClonedClassLoader extends ClassLoader {
      public ClonedClassLoader(ClassLoader cl) {
         super(cl);
      }
   }

}
