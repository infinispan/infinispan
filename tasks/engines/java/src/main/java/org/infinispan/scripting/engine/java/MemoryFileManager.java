package org.infinispan.scripting.engine.java;

import static javax.tools.StandardLocation.CLASS_OUTPUT;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.tools.FileObject;
import javax.tools.ForwardingJavaFileManager;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;

import org.infinispan.scripting.engine.java.util.CompositeIterator;

/**
 * A {@link JavaFileManager} that manages some files in memory, delegating the other files to the parent {@link
 * JavaFileManager}.
 * @author Eric Obermühlner
 */
public class MemoryFileManager extends ForwardingJavaFileManager<JavaFileManager> {

   private final Map<String, ClassMemoryJavaFileObject> mapNameToClasses = new HashMap<>();
   private final ClassLoader parentClassLoader;

   /**
    * Creates a MemoryJavaFileManager.
    *
    * @param fileManager       the {@link JavaFileManager}
    * @param parentClassLoader the parent {@link ClassLoader}
    */
   public MemoryFileManager(JavaFileManager fileManager, ClassLoader parentClassLoader) {
      super(fileManager);

      this.parentClassLoader = parentClassLoader;
   }

   private Collection<ClassMemoryJavaFileObject> memoryClasses() {
      return mapNameToClasses.values();
   }

   public JavaFileObject createSourceFileObject(Object origin, String name, String code) {
      return new MemoryJavaFileObject(origin, name, JavaFileObject.Kind.SOURCE, code);
   }

   public ClassLoader getClassLoader(JavaFileManager.Location location) {
      ClassLoader classLoader = super.getClassLoader(location);

      if (location == CLASS_OUTPUT) {
         if (parentClassLoader != null) {
            classLoader = parentClassLoader;
         }

         Map<String, byte[]> mapNameToBytes = new HashMap<>();

         for (ClassMemoryJavaFileObject outputMemoryJavaFileObject : memoryClasses()) {
            mapNameToBytes.put(
                  outputMemoryJavaFileObject.getName(),
                  outputMemoryJavaFileObject.getBytes());
         }

         return new MemoryClassLoader(mapNameToBytes, classLoader);
      }

      return classLoader;
   }

   @Override
   public Iterable<JavaFileObject> list(
         JavaFileManager.Location location,
         String packageName,
         Set<JavaFileObject.Kind> kinds,
         boolean recurse) throws IOException {
      Iterable<JavaFileObject> list = super.list(location, packageName, kinds, recurse);

      if (location == CLASS_OUTPUT) {
         Collection<? extends JavaFileObject> generatedClasses = memoryClasses();
         return () -> new CompositeIterator<JavaFileObject>(
               list.iterator(),
               generatedClasses.iterator());
      }

      return list;
   }

   @Override
   public String inferBinaryName(JavaFileManager.Location location, JavaFileObject file) {
      if (file instanceof ClassMemoryJavaFileObject) {
         return file.getName();
      } else {
         return super.inferBinaryName(location, file);
      }
   }

   @Override
   public JavaFileObject getJavaFileForOutput(
         JavaFileManager.Location location,
         String className,
         JavaFileObject.Kind kind,
         FileObject sibling)
         throws IOException {
      if (kind == JavaFileObject.Kind.CLASS) {
         ClassMemoryJavaFileObject file = new ClassMemoryJavaFileObject(className);
         mapNameToClasses.put(className, file);
         return file;
      }

      return super.getJavaFileForOutput(location, className, kind, sibling);
   }

   static abstract class AbstractMemoryJavaFileObject extends SimpleJavaFileObject {
      public AbstractMemoryJavaFileObject(String name, JavaFileObject.Kind kind) {
         super(URI.create("memory:///" +
               name.replace('.', '/') +
               kind.extension), kind);
      }
   }

   static class MemoryJavaFileObject extends AbstractMemoryJavaFileObject {
      private final Object origin;
      private final String code;

      MemoryJavaFileObject(Object origin, String className, JavaFileObject.Kind kind, String code) {
         super(className, kind);

         this.origin = origin;
         this.code = code;
      }

      public Object getOrigin() {
         return origin;
      }

      @Override
      public CharSequence getCharContent(boolean ignoreEncodingErrors) {
         return code;
      }
   }

   static class ClassMemoryJavaFileObject extends AbstractMemoryJavaFileObject {

      private final String className;
      private ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
      private transient byte[] bytes = null;

      public ClassMemoryJavaFileObject(String className) {
         super(className, JavaFileObject.Kind.CLASS);

         this.className = className;
      }

      public byte[] getBytes() {
         if (bytes == null) {
            bytes = byteOutputStream.toByteArray();
            byteOutputStream = null;
         }
         return bytes;
      }

      @Override
      public String getName() {
         return className;
      }

      @Override
      public OutputStream openOutputStream() {
         return byteOutputStream;
      }

      @Override
      public InputStream openInputStream() {
         return new ByteArrayInputStream(getBytes());
      }
   }

}
