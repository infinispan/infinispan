package org.infinispan.scripting.engine.java.name;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import javax.script.ScriptException;

import org.junit.Test;

public class DefaultNameStrategyTest {
   @Test
   public void testSimpleNames() throws ScriptException {
      DefaultNameStrategy nameStrategy = new DefaultNameStrategy();

      assertThat(nameStrategy.getFullName("public class Alpha {}")).isEqualTo("Alpha");
      assertThat(nameStrategy.getFullName("public class Beta$ {}")).isEqualTo("Beta$");
   }

   @Test
   public void testPackageAndNames() throws ScriptException {
      DefaultNameStrategy nameStrategy = new DefaultNameStrategy();

      assertThat(nameStrategy.getFullName("package com.example; public class Alpha {}")).isEqualTo("com.example.Alpha");
   }

   @Test
   public void testMultipleLines() throws ScriptException {
      DefaultNameStrategy nameStrategy = new DefaultNameStrategy();

      assertThat(nameStrategy.getFullName("" +
            "/* Comment */\n" +
            "package com.example;\n" +
            "public class Alpha {\n" +
            "}"))
            .isEqualTo("com.example.Alpha");


      assertThat(nameStrategy.getFullName("" +
            "/* Comment */\n" +
            "package \n" +
            "  com.example ;\n" +
            "\n" +
            "public\n" +
            "\tclass\n" +
            "\n" +
            "Alpha\n" +
            "{\n" +
            "}"))
            .isEqualTo("com.example.Alpha");
   }

   @Test
   public void failSimpleNames() {
      DefaultNameStrategy nameStrategy = new DefaultNameStrategy();

      assertThatThrownBy(() -> {
         nameStrategy.getFullName("");
      }).isInstanceOf(ScriptException.class);

      assertThatThrownBy(() -> {
         nameStrategy.getFullName("public class {}");
      }).isInstanceOf(ScriptException.class);

      assertThatThrownBy(() -> {
         nameStrategy.getFullName("class Alpha {}");
      }).isInstanceOf(ScriptException.class);
   }
}
