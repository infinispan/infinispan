package org.infinispan.component.processor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.AnnotationValue;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.AbstractAnnotationValueVisitor8;

public class AnnotationTypeValuesExtractor extends AbstractAnnotationValueVisitor8<Void, List<TypeMirror>> {
   public static List<TypeMirror> getTypeValues(TypeElement typeElement, TypeElement annotationType, String valueName) {
      ArrayList<TypeMirror> list = new ArrayList<>();
      AnnotationMirror annotation = getAnnotation(typeElement, annotationType);
      if (annotation != null) {
         AnnotationValue annotationValue = getAnnotationValue(annotation, valueName);
         if (annotationValue != null) {
            annotationValue.accept(new AnnotationTypeValuesExtractor(), list);
            return list;
         }
      }
      return list;
   }

   private static AnnotationMirror getAnnotation(TypeElement clazz, TypeElement annotationType) {
      List<? extends AnnotationMirror> annotationMirrors = clazz.getAnnotationMirrors();
      for (AnnotationMirror annotationMirror : annotationMirrors) {
         if (annotationMirror.getAnnotationType().equals(annotationType.asType())) {
            return annotationMirror;
         }
      }
      return null;
   }

   private static AnnotationValue getAnnotationValue(AnnotationMirror annotationMirror, String key) {
      for (Map.Entry<? extends ExecutableElement, ? extends AnnotationValue> entry : annotationMirror.getElementValues().entrySet()) {
         if (entry.getKey().getSimpleName().toString().equals(key)) {
            return entry.getValue();
         }
      }
      return null;
   }

   @Override
   public Void visitArray(List<? extends AnnotationValue> vals, List<TypeMirror> list) {
      for (AnnotationValue val : vals) {
         val.accept(this, list);
      }
      return null;
   }

   @Override
   public Void visitType(TypeMirror t, List<TypeMirror> list) {
      list.add(t);
      return null;
   }

   @Override
   public Void visitBoolean(boolean b, List<TypeMirror> p) {
      return null;
   }

   @Override
   public Void visitByte(byte b, List<TypeMirror> p) {
      return null;
   }

   @Override
   public Void visitChar(char c, List<TypeMirror> p) {
      return null;
   }

   @Override
   public Void visitDouble(double d, List<TypeMirror> p) {
      return null;
   }

   @Override
   public Void visitFloat(float f, List<TypeMirror> p) {
      return null;
   }

   @Override
   public Void visitInt(int i, List<TypeMirror> p) {
      return null;
   }

   @Override
   public Void visitLong(long i, List<TypeMirror> p) {
      return null;
   }

   @Override
   public Void visitShort(short s, List<TypeMirror> p) {
      return null;
   }

   @Override
   public Void visitString(String s, List<TypeMirror> p) {
      return null;
   }

   @Override
   public Void visitEnumConstant(VariableElement c, List<TypeMirror> p) {
      return null;
   }

   @Override
   public Void visitAnnotation(AnnotationMirror a, List<TypeMirror> p) {
      return null;
   }
}
