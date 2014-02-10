package org.infinispan.tools.doclet.jmx;

import com.sun.javadoc.AnnotationDesc;
import com.sun.javadoc.AnnotationTypeDoc;
import com.sun.javadoc.ClassDoc;
import com.sun.javadoc.DocErrorReporter;
import com.sun.javadoc.FieldDoc;
import com.sun.javadoc.MethodDoc;
import com.sun.javadoc.Parameter;
import com.sun.javadoc.RootDoc;
import com.sun.tools.doclets.formats.html.ConfigurationImpl;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.tools.doclet.html.HtmlGenerator;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * A Doclet that generates a guide to all JMX components exposed by Infinispan
 *
 * @author Manik Surtani
 * @since 4.0
 */
@SuppressWarnings("restriction")
public class JmxDoclet {
   static String outputDirectory = ".";
   static String header, footer, encoding, title, bottom;

   public static boolean start(RootDoc root) throws IOException {
      ClassDoc[] classes = root.classes();

      List<MBeanComponent> mbeans = new LinkedList<MBeanComponent>();

      for (ClassDoc cd : classes) {
         MBeanComponent mbean = toJmxComponent(cd);
         if (mbean != null) mbeans.add(mbean);
      }

      // sort components alphabetically
      Collections.sort(mbeans);

      HtmlGenerator generator = new JmxHtmlGenerator(encoding, jmxTitle(), bottom, footer, header, "JMX components exposed by Infinispan",
                                                     Arrays.asList("JMX", "Infinispan", "Data Grids", "Documentation", "Reference", "MBeans", "Management", "Console"),
                                                     mbeans);
      generator.generateHtml(outputDirectory + File.separator + "jmxComponents.html");

      return true;
   }

   private static String jmxTitle() {
      String s = "JMX Components";
      if (title == null || title.length() == 0)
         return s;
      else {
         s += " (" + title + ")";
         return s;
      }
   }

   public static int optionLength(String option) {
      return (createConfigurationImpl()).optionLength(option);
   }

   public static boolean validOptions(String options[][], DocErrorReporter reporter) {
      for (String[] option : options) {
//         System.out.println("  >> Option " + Arrays.toString(option));
         if (option[0].equals("-d")) outputDirectory = option[1];
         else if (option[0].equals("-encoding")) encoding = option[1];
         else if (option[0].equals("-bottom")) bottom = option[1];
         else if (option[0].equals("-footer")) footer = option[1];
         else if (option[0].equals("-header")) header = option[1];
         else if (option[0].equals("-doctitle")) title = option[1];
      }
      return (createConfigurationImpl()).validOptions(options, reporter);
   }

   private static ConfigurationImpl createConfigurationImpl() {
      try {
         // Deal with JDK7/JDK8 differences
         Method getInstanceMethod = ConfigurationImpl.class.getMethod("getInstance");
         return (ConfigurationImpl) getInstanceMethod.invoke(null);
      } catch (NoSuchMethodException e) {
         try {
            return ConfigurationImpl.class.newInstance();
         } catch (Exception e1) {
            throw new RuntimeException(e1);
         }
      } catch (Exception e1) {
         throw new RuntimeException(e1);
      }
   }

   private static MBeanComponent toJmxComponent(ClassDoc cd) {
      boolean isMBean = false;
      MBeanComponent mbc = new MBeanComponent();
      mbc.className = cd.qualifiedTypeName();
      mbc.name = cd.typeName();

      for (AnnotationDesc a : cd.annotations()) {
         AnnotationTypeDoc atd = a.annotationType();
         String annotationName = atd.qualifiedTypeName();

         if (annotationName.equals(MBean.class.getName())) {
            isMBean = true;
            setNameDesc(a.elementValues(), mbc);
         }
      }

      // now to test method level annotations
      for (MethodDoc method : cd.methods()) {
         for (AnnotationDesc a : method.annotations()) {
            String annotationName = a.annotationType().qualifiedTypeName();
            if (annotationName.equals(ManagedOperation.class.getName())) {
               isMBean = true;
               MBeanOperation o = new MBeanOperation();
               o.name = method.name();
               setNameDesc(a.elementValues(), o);
               o.returnType = method.returnType().simpleTypeName();
               for (Parameter p : method.parameters()) o.addParam(p.type().simpleTypeName());
               mbc.operations.add(o);


            } else if (annotationName.equals(ManagedAttribute.class.getName())) {
               isMBean = true;
               MBeanAttribute attr = new MBeanAttribute();

               // if this is a getter, look at the return type
               if (method.name().startsWith("get") || method.name().startsWith("is")) {
                  attr.type = method.returnType().simpleTypeName();
               } else if (method.parameters().length > 0) {
                  attr.type = method.parameters()[0].type().simpleTypeName();
               }

               attr.name = fromBeanConvention(method.name());
               setNameDesc(a.elementValues(), attr);
               setWritable(a.elementValues(), attr);
               mbc.attributes.add(attr);
            }
         }
      }

      // and field level annotations
      for (FieldDoc field : cd.fields(false)) {

         for (AnnotationDesc a : field.annotations()) {
            String annotationName = a.annotationType().qualifiedTypeName();

            if (annotationName.equals(ManagedAttribute.class.getName())) {
               isMBean = true;
               MBeanAttribute attr = new MBeanAttribute();
               attr.name = field.name();
               attr.type = field.type().simpleTypeName();
               setNameDesc(a.elementValues(), attr);
               setWritable(a.elementValues(), attr);
               mbc.attributes.add(attr);
            }
         }
      }

      if (isMBean) {
         Collections.sort(mbc.attributes);
         Collections.sort(mbc.operations);

         return mbc;
      } else {
         return null;
      }
   }

   private static String fromBeanConvention(String getterOrSetter) {
      if (getterOrSetter.startsWith("get") || getterOrSetter.startsWith("set")) {
         String withoutGet = getterOrSetter.substring(4);
         // not specifically BEAN convention, but this is what is bound in JMX.
         return Character.toUpperCase(getterOrSetter.charAt(3)) + withoutGet;
      } else if (getterOrSetter.startsWith("is")) {
         String withoutIs = getterOrSetter.substring(3);
         return Character.toUpperCase(getterOrSetter.charAt(2)) + withoutIs;
      }
      return getterOrSetter;
   }

   private static void setNameDesc(AnnotationDesc.ElementValuePair[] evps, JmxComponent mbc) {
      for (AnnotationDesc.ElementValuePair evp : evps) {
         if (evp.element().name().equals("objectName")) {
            mbc.name = evp.value().value().toString();
         } else if (evp.element().name().equals("name")) {
            mbc.name = evp.value().value().toString();
         } else if (evp.element().name().equals("description")) {
            mbc.desc = evp.value().value().toString();
         }
      }
   }

   private static void setWritable(AnnotationDesc.ElementValuePair[] evps, MBeanAttribute attr) {
      for (AnnotationDesc.ElementValuePair evp : evps) {
         if (evp.element().name().equals("writable")) {
            attr.writable = (Boolean) evp.value().value();
         }
      }
   }
}

