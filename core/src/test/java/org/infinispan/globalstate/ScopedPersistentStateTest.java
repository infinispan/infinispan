package org.infinispan.globalstate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.infinispan.globalstate.impl.ScopedPersistentStateImpl;
import org.testng.annotations.Test;

@Test(testName = "globalstate.ScopedPersistentStateTest", groups = "functional")
public class ScopedPersistentStateTest {

   public void testStateChecksum() {
      ScopedPersistentState state1 = new ScopedPersistentStateImpl("scope");
      state1.setProperty("a", "a");
      state1.setProperty("b", 1);
      state1.setProperty("c", 2.0f);
      state1.setProperty("@local", "state1");
      ScopedPersistentState state2 = new ScopedPersistentStateImpl("scope");
      state2.setProperty("a", "a");
      state2.setProperty("b", 1);
      state2.setProperty("c", 2.0f);
      state2.setProperty("@local", "state2");
      assertEquals(state1.getChecksum(), state2.getChecksum());
      ScopedPersistentState state3 = new ScopedPersistentStateImpl("scope");
      state3.setProperty("a", "x");
      state3.setProperty("b", 1);
      state3.setProperty("c", 2.0f);
      state3.setProperty("@local", "state1");
      assertNotEquals(state1.getChecksum(), state3.getChecksum());
   }
}
