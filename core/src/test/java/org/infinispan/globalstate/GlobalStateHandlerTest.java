package org.infinispan.globalstate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.testing.Testing.tmpDirectory;

import java.io.File;
import java.util.Optional;

import org.infinispan.commons.time.DefaultTimeService;
import org.infinispan.globalstate.impl.GlobalStateHandler;
import org.infinispan.globalstate.impl.ScopedPersistentStateImpl;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(testName = "globalstate.GlobalStateHandlerTest", groups = "functional")
public class GlobalStateHandlerTest extends AbstractInfinispanTest {

   @DataProvider(name = "problematicScopeNames")
   public Object[][] problematicScopeNames() {
      return new Object[][] {
            {"testScopeWithSlashes", "___local-attrs.test/cache/with/slashes"},
            {"testScopeWithPathTraversal", "/../../../../../cache/with/slashes"},
            {"testScopeWithOtherProblematicChars", "scope:with\\various<problem>chars"},
      };
   }

   @Test(dataProvider = "problematicScopeNames")
   public void testScopeSanitization(String testName, String scope) {
      String tmpDir = tmpDirectory(this.getClass().getSimpleName(), testName);
      new File(tmpDir).mkdirs();
      GlobalStateHandler handler = new GlobalStateHandler(tmpDir, DefaultTimeService.INSTANCE);

      // Create and write state
      ScopedPersistentState writeState = new ScopedPersistentStateImpl(scope);
      writeState.setProperty("key1", "value1");
      writeState.setProperty("key2", "value2");

      // Handler should sanitize the scope for filesystem use
      handler.writeScopedState(writeState);

      // Handler should use same sanitization when reading
      Optional<ScopedPersistentState> readState = handler.readScopedState(scope);
      assertThat(readState).isPresent();
      assertThat(readState.get().getProperty("key1")).isEqualTo("value1");
      assertThat(readState.get().getProperty("key2")).isEqualTo("value2");

      // Cleanup should also work
      handler.deleteScopedState(scope);
      Optional<ScopedPersistentState> afterDelete = handler.readScopedState(scope);
      assertThat(afterDelete).isEmpty();
   }

   public void testScopeCollisionAvoidance() {
      String tmpDir = tmpDirectory(this.getClass().getSimpleName(), "testScopeCollisionAvoidance");
      new File(tmpDir).mkdirs();
      GlobalStateHandler handler = new GlobalStateHandler(tmpDir, DefaultTimeService.INSTANCE);

      // Two different scopes that might collide if naively escaped
      String scope1 = "cache/name";
      String scope2 = "cache:name";

      ScopedPersistentState state1 = new ScopedPersistentStateImpl(scope1);
      state1.setProperty("id", "first");

      ScopedPersistentState state2 = new ScopedPersistentStateImpl(scope2);
      state2.setProperty("id", "second");

      handler.writeScopedState(state1);
      handler.writeScopedState(state2);

      // Both should be readable independently without collision
      Optional<ScopedPersistentState> read1 = handler.readScopedState(scope1);
      Optional<ScopedPersistentState> read2 = handler.readScopedState(scope2);

      assertThat(read1).isPresent();
      assertThat(read1.get().getProperty("id")).isEqualTo("first");

      assertThat(read2).isPresent();
      assertThat(read2.get().getProperty("id")).isEqualTo("second");
   }
}
