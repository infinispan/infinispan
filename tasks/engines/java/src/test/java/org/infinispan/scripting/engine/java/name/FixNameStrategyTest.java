package org.infinispan.scripting.engine.java.name;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class FixNameStrategyTest {
   @Test
   public void testSimpleName() {
      String script = "script";
      FixNameStrategy nameStrategy = new FixNameStrategy("Test");
      String fullName = nameStrategy.getFullName(script);

      assertThat(fullName).isEqualTo("Test");
      assertThat(NameStrategy.extractSimpleName(fullName)).isEqualTo("Test");
      assertThat(NameStrategy.extractPackageName(fullName)).isEqualTo("");
   }

   @Test
   public void testFullyQualifiedName() {
      String script = "script";
      FixNameStrategy nameStrategy = new FixNameStrategy("com.example.Test");
      String fullName = nameStrategy.getFullName(script);

      assertThat(fullName).isEqualTo("com.example.Test");
      assertThat(NameStrategy.extractSimpleName(fullName)).isEqualTo("Test");
      assertThat(NameStrategy.extractPackageName(fullName)).isEqualTo("com.example");
   }

}
