package org.infinispan.component.processor.external;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Instant;
import java.util.Set;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.DeclaredType;

import org.infinispan.external.JGroupsProtocolComponent;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.stack.Protocol;
import org.kohsuke.MetaInfServices;

/**
 * @since 14.0
 **/
@SupportedSourceVersion(SourceVersion.RELEASE_8)
@SupportedAnnotationTypes("org.infinispan.external.JGroupsProtocolComponent")
@MetaInfServices(Processor.class)
public class JGroupsComponentProcessor extends AbstractProcessor {
   @Override
   public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
      for (TypeElement annotation : annotations) {
         for (Element element : roundEnv.getElementsAnnotatedWith(annotation)) {
            JGroupsProtocolComponent component = element.getAnnotation(JGroupsProtocolComponent.class);
            DeclaredType typeMirror = (DeclaredType) element.asType();
            TypeElement type = (TypeElement) typeMirror.asElement();
            String fqcn = type.getQualifiedName().toString();
            int lastDot = fqcn.lastIndexOf('.');
            String packageName = fqcn.substring(0, lastDot);
            try (PrintWriter w = new PrintWriter(processingEnv.getFiler().createSourceFile(packageName + '.' + component.value()).openWriter())) {
               generate(w, packageName, component.value());
            } catch (IOException e) {
               throw new RuntimeException(e);
            }
         }
      }

      return true;
   }

   public void generate(PrintWriter w, String packageName, String className) throws IOException {
      w.printf("package %s;%n", packageName);
      w.println("import java.util.ArrayList;");
      w.println("import java.util.Collection;");
      w.println("import java.util.HashMap;");
      w.println("import java.util.List;");
      w.println("import java.util.Map;");
      w.println("import java.util.function.Function;");
      w.println("import javax.annotation.Generated;");
      w.println("import static org.infinispan.factories.impl.MBeanMetadata.AttributeMetadata;");
      w.println("import org.jgroups.stack.Protocol;");
      w.println();
      w.printf("@Generated(value = \"%s\", date = \"%s\")%n", getClass().getName(), Instant.now().toString());
      w.printf("public class %s {%n", className);
      w.println("   public static final Map<Class<? extends Protocol>, Collection<AttributeMetadata>> PROTOCOL_METADATA = new HashMap<>();");
      w.printf("   private %s() {}%n", className);
      w.println("   static {");
      w.println("      List<AttributeMetadata> attributes;");
      for (short id = 0; id < 256; id++) {
         Class protocol = ClassConfigurator.getProtocol(id);
         if (protocol != null && Protocol.class.isAssignableFrom(protocol)) {
            w.println("      attributes = new ArrayList<>();");
            for (Method method : protocol.getMethods()) {
               ManagedAttribute annotation = method.getAnnotation(ManagedAttribute.class);
               if (annotation != null && !Modifier.isStatic(method.getModifiers()) && method.getParameterCount() == 0 && isNumber(method.getReturnType())) {
                  w.printf("      attributes.add(new AttributeMetadata(\"%s\", \"%s\", false, false, \"%s\",\n" +
                              "                               false, (Function<%s, ?>) %s::%s, null));%n",
                        method.getName(), annotation.description().replace('"', '\''), method.getReturnType().getName(), protocol.getName(), protocol.getName(), method.getName());
               }
            }
            w.printf("      PROTOCOL_METADATA.put(%s.class, attributes);%n", protocol.getName());
         }
      }

      w.println("   }");
      w.println("}");
      w.println();
      w.flush();
      w.close();
   }

   private static boolean isNumber(Class<?> type) {
      return short.class == type || byte.class == type || long.class == type || int.class == type || float.class == type || double.class == type || Number.class.isAssignableFrom(type);
   }
}
