package org.infinispan.cdi.util.annotatedtypebuilder;

import java.lang.reflect.Member;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;

import javax.enterprise.inject.spi.AnnotatedMember;
import javax.enterprise.inject.spi.AnnotatedType;

/**
 * @author Stuart Douglas
 */
abstract class AnnotatedMemberImpl<X, M extends Member> extends AnnotatedImpl implements AnnotatedMember<X> {

   private final AnnotatedType<X> declaringType;
   private final M javaMember;

   protected AnnotatedMemberImpl(AnnotatedType<X> declaringType, M member, Class<?> memberType, AnnotationStore annotations, Type genericType, Type overridenType) {
      super(memberType, annotations, genericType, overridenType);
      this.declaringType = declaringType;
      this.javaMember = member;
   }

   public AnnotatedType<X> getDeclaringType() {
      return declaringType;
   }

   public M getJavaMember() {
      return javaMember;
   }

   public boolean isStatic() {
      return Modifier.isStatic(javaMember.getModifiers());
   }

}
