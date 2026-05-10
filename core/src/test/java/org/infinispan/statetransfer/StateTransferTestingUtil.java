package org.infinispan.statetransfer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.infinispan.Cache;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.test.TestingUtil;

/**
 * StateTransferTestingUtil.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
public class StateTransferTestingUtil {
   public static final String A_B_NAME = "a_b_name";
   public static final String A_C_NAME = "a_c_name";
   public static final String A_D_NAME = "a_d_age";
   public static final String A_B_AGE = "a_b_age";
   public static final String A_C_AGE = "a_c_age";
   public static final String A_D_AGE = "a_d_age";
   public static final String JOE = "JOE";
   public static final String BOB = "BOB";
   public static final String JANE = "JANE";
   public static final Integer TWENTY = 20;
   public static final Integer FORTY = 40;

   public static void verifyNoDataOnLoader(Cache<Object, Object> c) throws Exception {
      DummyInMemoryStore l = TestingUtil.getFirstStore(c);
      assertFalse(l.contains(A_B_AGE));
      assertFalse(l.contains(A_B_NAME));
      assertFalse(l.contains(A_C_AGE));
      assertFalse(l.contains(A_C_NAME));
      assertFalse(l.contains(A_D_AGE));
      assertFalse(l.contains(A_D_NAME));
   }

   public static void verifyNoData(Cache<Object, Object> c) {
      assertTrue(c.isEmpty(), "Cache should be empty!");
   }

   public static void writeInitialData(final Cache<Object, Object> c) {
      c.put(A_B_NAME, JOE);
      c.put(A_B_AGE, TWENTY);
      c.put(A_C_NAME, BOB);
      c.put(A_C_AGE, FORTY);
      c.evict(A_B_NAME);
      c.evict(A_B_AGE);
      c.evict(A_C_NAME);
      c.evict(A_C_AGE);
      c.evict(A_D_NAME);
      c.evict(A_D_AGE);
   }

   public static void verifyInitialDataOnLoader(Cache<Object, Object> c) throws Exception {
      DummyInMemoryStore l = TestingUtil.getFirstStore(c);
      assertTrue(l.contains(A_B_AGE));
      assertTrue(l.contains(A_B_NAME));
      assertTrue(l.contains(A_C_AGE));
      assertTrue(l.contains(A_C_NAME));
      assertEquals(TWENTY, l.loadEntry(A_B_AGE).getValue());
      assertEquals(JOE, l.loadEntry(A_B_NAME).getValue());
      assertEquals(FORTY, l.loadEntry(A_C_AGE).getValue());
      assertEquals(BOB, l.loadEntry(A_C_NAME).getValue());
   }

   public static void verifyInitialData(Cache<Object, Object> c) {
      assertEquals(c.get(A_B_NAME), JOE, "Incorrect value for key " + A_B_NAME);
      assertEquals(c.get(A_B_AGE), TWENTY, "Incorrect value for key " + A_B_AGE);
      assertEquals(c.get(A_C_NAME), BOB, "Incorrect value for key " + A_C_NAME);
      assertEquals(c.get(A_C_AGE), FORTY, "Incorrect value for key " + A_C_AGE);
   }
}
