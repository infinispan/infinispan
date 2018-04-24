package org.infinispan.doclets.api;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;

import com.sun.javadoc.Doc;
import com.sun.javadoc.ProgramElementDoc;

public class PublicAPIFilterHandler implements InvocationHandler {
   private static final String PUBLIC_TAG = "@public";
   private static final String PRIVATE_TAG = "@private";
   private Object target;

   public PublicAPIFilterHandler(Object target) {
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
         return ((PublicAPIFilterHandler) Proxy.getInvocationHandler(proxy)).target;
      return proxy;
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

   public static Object filter(Object obj, Class<?> expect) {
      if (obj == null)
         return null;
      Class<?> cls = obj.getClass();
      if (cls.getName().startsWith("com.sun.")) {
         return Proxy.newProxyInstance(cls.getClassLoader(), cls.getInterfaces(), new PublicAPIFilterHandler(obj));
      } else if (obj instanceof Object[]) {
         Class<?> componentType = expect.getComponentType();
         if (componentType == null) {
            return obj;
         }
         String componentName = componentType.getName();
         if (!componentName.startsWith("com.sun.javadoc"))
            return obj;

         Object[] array = (Object[]) obj;
         List<Object> list = new ArrayList<>(array.length);
         for (int i = 0; i < array.length; i++) {
            Object entry = array[i];
            if ((entry instanceof Doc) && !isPublicAPI((Doc) entry))
               continue;
            list.add(entry);
         }
         return list.toArray((Object[]) Array.newInstance(componentType, list.size()));
      } else {
         return obj;
      }

   }
}
