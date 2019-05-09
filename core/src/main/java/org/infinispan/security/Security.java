package org.infinispan.security;

import java.security.AccessControlContext;
import java.security.AccessControlException;
import java.security.AccessController;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.security.acl.Group;
import java.util.ArrayDeque;
import java.util.Deque;

import javax.security.auth.Subject;

import org.infinispan.commons.jdkspecific.CallerId;

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
public final class Security {

   private static final ThreadLocal<Boolean> PRIVILEGED = ThreadLocal.withInitial(() -> Boolean.FALSE);

   private static final ThreadLocal<Deque<Subject>> SUBJECT = new ThreadLocal<>();

   private static boolean isTrustedClass(Class<?> klass) {
      // TODO: implement a better way
      return klass.getPackage().getName().startsWith("org.infinispan.");
   }

   public static <T> T doPrivileged(PrivilegedAction<T> action) {
      if (!isPrivileged() && isTrustedClass(CallerId.getCallerClass(3))) {
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
      if (!isPrivileged() && isTrustedClass(CallerId.getCallerClass(3))) {
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
      Deque<Subject> stack = SUBJECT.get();
      if (stack == null) {
         stack = new ArrayDeque<>();
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
      Deque<Subject> stack = SUBJECT.get();
      if (stack == null) {
         stack = new ArrayDeque<>();
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
         if (System.getSecurityManager() == null) {
            return Subject.getSubject(acc);
         } else {
            return AccessController.doPrivileged((PrivilegedAction<Subject>) () -> Subject.getSubject(acc));
         }
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
}
