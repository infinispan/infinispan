package org.infinispan.security;

import java.security.AccessControlContext;
import java.security.AccessControlException;
import java.security.AccessController;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.security.acl.Group;
import java.util.Stack;

import javax.security.auth.Subject;

import sun.reflect.Reflection;

/**
 * Security. A simple class to implement caller privileges without a security manager and a
 * much faster implementations of the {@link Subject#doAs(Subject, PrivilegedAction)} and
 * {@link Subject#doAs(Subject, PrivilegedExceptionAction)} when interaction with the
 * {@link AccessControlContext} is not needed.
 *
 * N.B. this uses the caller's {@link Package}, this can easily be subverted by placing the
 * calling code within the org.infinispan hierarchy. However for most purposes this is ok.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
@SuppressWarnings({ "restriction", "deprecation" })
public final class Security {
   private static final boolean hasGetCallerClass;
   private static final int callerOffset;
   private static final LocalSecurityManager SECURITY_MANAGER;

   static {
      boolean result = false;
      int offset = 0;
      try {
         result = Reflection.getCallerClass(1) == Security.class || Reflection.getCallerClass(2) == Security.class;
         offset = Reflection.getCallerClass(1) == Reflection.class ? 2 : 1;
      } catch (Throwable ignored) {
      }
      hasGetCallerClass = result;
      callerOffset = offset;
      if (!hasGetCallerClass) {
         SECURITY_MANAGER = new LocalSecurityManager();
      } else {
         SECURITY_MANAGER = null;
      }
   }

   private static class LocalSecurityManager extends SecurityManager {
      public Class<?>[] getClasses() {
          return this.getClassContext();
      }
  }

   private static final ThreadLocal<Boolean> PRIVILEGED = new ThreadLocal<Boolean>() {
      @Override
      protected Boolean initialValue() {
         return Boolean.FALSE;
      }
   };

   private static final ThreadLocal<Stack<Subject>> SUBJECT = new ThreadLocal<Stack<Subject>>();

   private static boolean isTrustedClass(Class<?> klass) {
      // TODO: implement a better way
      return klass.getPackage().getName().startsWith("org.infinispan.");
   }

   public static <T> T doPrivileged(PrivilegedAction<T> action) {
      if (!isPrivileged() && isTrustedClass(getCallerClass(2))) {
         try {
            PRIVILEGED.set(true);
            return action.run();
         } finally {
            PRIVILEGED.remove();
         }
      } else {
         return action.run();
      }
   }

   public static <T> T doPrivileged(PrivilegedExceptionAction<T> action) throws PrivilegedActionException {
      if (!isPrivileged() && isTrustedClass(getCallerClass(2))) {
         try {
            PRIVILEGED.set(true);
            return action.run();
         } catch (Exception e) {
            throw new PrivilegedActionException(e);
         } finally {
            PRIVILEGED.remove();
         }
      } else {
         try {
            return action.run();
         } catch (Exception e) {
            throw new PrivilegedActionException(e);
         }
      }
   }

   /**
    * A "lightweight" implementation of {@link Subject#doAs(Subject, PrivilegedAction)} which uses a ThreadLocal
    * {@link Subject} instead of modifying the current {@link AccessControlContext}.
    *
    * @see Subject#doAs(Subject, PrivilegedAction)
    */
   public static <T> T doAs(final Subject subject, final java.security.PrivilegedAction<T> action) {
      Stack<Subject> stack = SUBJECT.get();
      if (stack == null) {
         stack = new Stack<Subject>();
         SUBJECT.set(stack);
      }
      stack.push(subject);
      try {
         return action.run();
      } finally {
         stack.pop();
         if (stack.isEmpty()) {
            SUBJECT.remove();
         }
      }
   }

   /**
    * A "lightweight" implementation of {@link Subject#doAs(Subject, PrivilegedExceptionAction)} which uses a ThreadLocal
    * {@link Subject} instead of modifying the current {@link AccessControlContext}.
    *
    * @see Subject#doAs(Subject, PrivilegedExceptionAction)
    */
   public static <T> T doAs(final Subject subject,
         final java.security.PrivilegedExceptionAction<T> action)
         throws java.security.PrivilegedActionException {
      Stack<Subject> stack = SUBJECT.get();
      if (stack == null) {
         stack = new Stack<Subject>();
         SUBJECT.set(stack);
      }
      stack.push(subject);
      try {
         return action.run();
      } catch (Exception e) {
         throw new PrivilegedActionException(e);
      } finally {
         stack.pop();
         if (stack.isEmpty()) {
            SUBJECT.remove();
         }
      }
   }


   public static void checkPermission(CachePermission permission) throws AccessControlException {
      if (!isPrivileged()) {
         throw new AccessControlException("Call from unprivileged code", permission);
      }
   }

   public static boolean isPrivileged() {
      return PRIVILEGED.get();
   }

   /**
    * If using {@link Security#doAs(Subject, PrivilegedAction)} or
    * {@link Security#doAs(Subject, PrivilegedExceptionAction)}, returns the {@link Subject} associated with the current thread
    * otherwise it returns the {@link Subject} associated with the current {@link AccessControlContext}
    */
   public static Subject getSubject() {
      if (SUBJECT.get() != null) {
         return SUBJECT.get().peek();
      } else {
         AccessControlContext acc = AccessController.getContext();
         return Subject.getSubject(acc);
      }
   }

   /**
    * Returns the first principal of a subject which is not of type {@link java.security.acl.Group}
    */
   public static Principal getSubjectUserPrincipal(Subject s) {
      if (s != null) {
         for (Principal p : s.getPrincipals()) {
            if (!(p instanceof Group)) {
               return p;
            }
         }
      }
      return null;
   }

   static Class<?> getCallerClass(int n) {
      if (hasGetCallerClass) {
         return Reflection.getCallerClass(n + callerOffset);
      } else {
         return SECURITY_MANAGER.getClasses()[n + callerOffset];
      }
   }
}
