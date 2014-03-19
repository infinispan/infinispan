package org.infinispan.tools.doclet;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;

import com.sun.javadoc.Doc;
import com.sun.javadoc.DocErrorReporter;
import com.sun.javadoc.LanguageVersion;
import com.sun.javadoc.ProgramElementDoc;
import com.sun.javadoc.RootDoc;
import com.sun.tools.doclets.standard.Standard;
import com.sun.tools.javadoc.Main;

public class PublicAPI {
   private static final String PUBLIC_TAG = "@public";
   private static final String PRIVATE_TAG = "@private";

   public static void main(String[] args) {
      String name = PublicAPI.class.getName();
      Main.execute(name, name, args);
   }

   public static boolean validOptions(String[][] options, DocErrorReporter reporter) throws java.io.IOException {
      return Standard.validOptions(options, reporter);
   }

   public static LanguageVersion languageVersion() {
      return LanguageVersion.JAVA_1_5;
   }

   public static int optionLength(String option) {
      return Standard.optionLength(option);
   }

   public static boolean start(RootDoc root) throws java.io.IOException {
      return Standard.start((RootDoc) filter(root, RootDoc.class));
   }

   private static boolean isPublicAPI(Doc doc) {
      if (doc.tags(PRIVATE_TAG).length > 0)
         return false;
      if (doc instanceof ProgramElementDoc) {
         ProgramElementDoc peDoc = (ProgramElementDoc) doc;
         if (peDoc.containingClass() != null && peDoc.containingClass().tags(PUBLIC_TAG).length > 0)
            return true;
         if (peDoc.containingPackage().tags(PUBLIC_TAG).length > 0)
            return true;
      }
      return doc.tags(PUBLIC_TAG).length > 0;
   }

   private static Object filter(Object obj, Class expect) {
      if (obj == null)
         return null;
      Class cls = obj.getClass();
      if (cls.getName().startsWith("com.sun.")) {
         return Proxy.newProxyInstance(cls.getClassLoader(), cls.getInterfaces(), new FilterHandler(obj));
      } else if (obj instanceof Object[]) {
         Class componentType = expect.getComponentType();
         Object[] array = (Object[]) obj;
         List list = new ArrayList(array.length);
         for (int i = 0; i < array.length; i++) {
            Object entry = array[i];
            if ((entry instanceof Doc) && !isPublicAPI((Doc) entry))
               continue;
            list.add(filter(entry, componentType));
         }
         return list.toArray((Object[]) Array.newInstance(componentType, list.size()));
      } else {
         return obj;
      }
   }

   private static class FilterHandler implements InvocationHandler {
      private Object target;

      public FilterHandler(Object target) {
         this.target = target;
      }

      @Override
      public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
         if (args != null) {
            String methodName = method.getName();
            if (methodName.equals("compareTo") || methodName.equals("equals") || methodName.equals("overrides")
                  || methodName.equals("subclassOf")) {
               args[0] = unwrap(args[0]);
            }
         }
         try {
            return filter(method.invoke(target, args), method.getReturnType());
         } catch (InvocationTargetException e) {
            throw e.getTargetException();
         }
      }

      private Object unwrap(Object proxy) {
         if (proxy instanceof Proxy)
            return ((FilterHandler) Proxy.getInvocationHandler(proxy)).target;
         return proxy;
      }
   }

}