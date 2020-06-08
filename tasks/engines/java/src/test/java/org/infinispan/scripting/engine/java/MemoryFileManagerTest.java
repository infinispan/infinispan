package org.infinispan.scripting.engine.java;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.StandardLocation;
import javax.tools.ToolProvider;

import org.junit.Test;

public class MemoryFileManagerTest {
   @Test
   public void testCreateSourceFileObject() throws IOException {
      JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
      StandardJavaFileManager standardFileManager = compiler.getStandardFileManager(null, null, null);
      try (MemoryFileManager memoryFileManager = new MemoryFileManager(standardFileManager, null)) {
         String code = "public class XXX {}";
         JavaFileObject javaFileObject = memoryFileManager.createSourceFileObject("origin", "XXX", code);

         assertThat(javaFileObject.getKind()).isEqualTo(JavaFileObject.Kind.SOURCE);
         assertThat(javaFileObject.getCharContent(true)).isEqualTo(code);
         assertThat(javaFileObject).isInstanceOf(MemoryFileManager.MemoryJavaFileObject.class);

         MemoryFileManager.MemoryJavaFileObject memoryJavaFileObject = (MemoryFileManager.MemoryJavaFileObject) javaFileObject;
         assertThat(memoryJavaFileObject.getOrigin()).isEqualTo("origin");
      }
   }


   @Test
   public void testGetJavaFileForOutput() throws IOException {
      JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
      StandardJavaFileManager standardFileManager = compiler.getStandardFileManager(null, null, null);
      try (MemoryFileManager memoryFileManager = new MemoryFileManager(standardFileManager, null)) {
         JavaFileObject javaFileObject = memoryFileManager.getJavaFileForOutput(StandardLocation.CLASS_OUTPUT, "XXX", JavaFileObject.Kind.CLASS, null);
         assertThat(javaFileObject).isInstanceOf(MemoryFileManager.ClassMemoryJavaFileObject.class);

         String name = memoryFileManager.inferBinaryName(StandardLocation.CLASS_OUTPUT, javaFileObject);
         assertThat(name).isEqualTo(javaFileObject.getName());
      }
   }

   @Test
   public void testList() throws IOException, ClassNotFoundException {
      JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
      StandardJavaFileManager standardFileManager = compiler.getStandardFileManager(null, null, null);
      try (MemoryFileManager memoryFileManager = new MemoryFileManager(standardFileManager, null)) {
         JavaFileObject javaFileObject = memoryFileManager.getJavaFileForOutput(StandardLocation.CLASS_OUTPUT, "XXX", JavaFileObject.Kind.CLASS, null);

         Iterable<JavaFileObject> result = memoryFileManager.list(
               StandardLocation.CLASS_OUTPUT,
               "",
               new HashSet<>(Arrays.asList(JavaFileObject.Kind.CLASS)), true);
         assertThat(result).containsExactly(javaFileObject);
      }
   }

   @Test
   public void testClassLoader() throws IOException, ClassNotFoundException {
      JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
      StandardJavaFileManager standardFileManager = compiler.getStandardFileManager(null, null, null);
      try (MemoryFileManager memoryFileManager = new MemoryFileManager(standardFileManager, null)) {
         memoryFileManager.getJavaFileForOutput(StandardLocation.CLASS_OUTPUT, "XXX", JavaFileObject.Kind.CLASS, null);

         ClassLoader classLoader = memoryFileManager.getClassLoader(StandardLocation.CLASS_OUTPUT);
         assertThatThrownBy(() -> {
            classLoader.loadClass("XXX");
         }).isInstanceOf(java.lang.ClassFormatError.class);
      }
   }
}
