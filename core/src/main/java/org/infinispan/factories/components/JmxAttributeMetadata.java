package org.infinispan.factories.components;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.infinispan.commons.util.ReflectionUtil;
import org.infinispan.jmx.annotations.ManagedAttribute;

/**
 * Persistable and cacheable metadata for JMX attributes
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class JmxAttributeMetadata implements Serializable {
   private static final long serialVersionUID = 0x1114A77121B07EL;
   private String name;
   private String description;
   private boolean writable;
   private boolean useSetter;
   private String type;
   private boolean is;

   private JmxAttributeMetadata(ManagedAttribute annotation) {
      description = annotation.description();
      writable = annotation.writable();
   }

   public JmxAttributeMetadata(Field field) {
      this(field.getAnnotation(ManagedAttribute.class));
      name = field.getName();
      type = field.getType().toString();
   }

   public JmxAttributeMetadata(Method method) {
      this(method.getAnnotation(ManagedAttribute.class));
      useSetter = true;
      String methodName = method.getName();
      name = ReflectionUtil.extractFieldName(methodName);
      is = methodName.startsWith("is");
      if (methodName.startsWith("set")) {
         type = method.getParameterTypes()[0].getName();
      } else if (methodName.startsWith("get") || is) {
         type = method.getReturnType().getName();
      }
   }

   public String getName() {
      return name;
   }

   public String getDescription() {
      return description;
   }

   public boolean isWritable() {
      return writable;
   }

   public boolean isUseSetter() {
      return useSetter;
   }

   public String getType() {
      return type;
   }

   public boolean isIs() {
      return is;
   }

   @Override
   public String toString() {
      return "JmxAttributeMetadata{" +
            "name='" + name + '\'' +
            ", description='" + description + '\'' +
            ", writable=" + writable +
            ", type='" + type + '\'' +
            ", is=" + is +
            '}';
   }
}
