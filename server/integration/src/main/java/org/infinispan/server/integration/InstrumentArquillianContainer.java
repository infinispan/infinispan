package org.infinispan.server.integration;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

import org.jboss.arquillian.container.test.api.Deployment;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.asm.AsmVisitorWrapper;
import net.bytebuddy.asm.MemberAttributeExtension;
import net.bytebuddy.description.annotation.AnnotationDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.loading.ClassReloadingStrategy;
import net.bytebuddy.matcher.ElementMatchers;

public class InstrumentArquillianContainer {

   static {
      // deploy the war when there is a server
      if (!ArquillianServerType.NONE.equals(ArquillianServerType.current())) {
         net.bytebuddy.agent.ByteBuddyAgent.install();
         DynamicType.Builder<?> builder = new ByteBuddy().redefine(getBaseItClass());
         builder.visit(create("createDeployment", Deployment.class))
               .make()
               .load(InstrumentedArquillianTestClass.class.getClassLoader(), ClassReloadingStrategy.fromInstalledAgent()).getLoaded();
      }
   }

   private static AsmVisitorWrapper create(String baseMethod, Class<? extends Annotation> annotation) {
      try {
         Method existingMethod = getBaseItClass().getMethod(baseMethod);
         AnnotationDescription annotationDescription = AnnotationDescription.Builder.ofType(annotation).build();
         AsmVisitorWrapper visit = new MemberAttributeExtension.ForMethod().annotateMethod(annotationDescription).on(ElementMatchers.anyOf(existingMethod));
         return visit;
      } catch (NoSuchMethodException e) {
         throw new IllegalStateException("Method not found", e);
      }
   }

   private static Class<?> getBaseItClass() {
      try {
         return Class.forName("org.infinispan.server.integration.BaseIT");
      } catch (ClassNotFoundException e) {
         throw new IllegalStateException("Cannot found BaseIT", e);
      }
   }
}
