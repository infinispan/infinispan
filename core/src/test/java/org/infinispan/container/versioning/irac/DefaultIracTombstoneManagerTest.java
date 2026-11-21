package org.infinispan.container.versioning.irac;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.testng.annotations.Test;

@Test(groups = "unit", testName = "container.versioning.irac.DefaultIracTombstoneManagerTest")
public class DefaultIracTombstoneManagerTest {

   public void testSanitizeExceptionForLog() {
      String message = DefaultIracTombstoneManager.sanitizeExceptionForLog(
            new IllegalStateException("peer failure\r\nforged entry\twith control\u0000"));

      assertEquals("java.lang.IllegalStateException: peer failure  forged entry with control ", message);
      assertFalse(message.chars().anyMatch(Character::isISOControl));
   }
}
