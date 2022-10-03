package org.infinispan.security;

import java.security.AccessControlContext;
import java.security.AccessControlException;
import java.security.AccessController;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.function.BiFunction;
import java.util.function.Function;

import javax.security.auth.Subject;

import org.infinispan.commons.jdkspecific.CallerId;

/**
 * Security. A simple class to implement caller privileges without a security manager and a much faster implementations
 * of the {@link Subject#doAs(Subject, PrivilegedAction)} and {@link Subject#doAs(Subject, PrivilegedExceptionAction)}
 * when interaction with the {@link AccessControlContext} is not needed.
 * <p>
 * N.B. this uses the caller's {@link Package}, this can easily be subverted by placing the calling code within the
 * org.infinispan hierarchy. However for most purposes this is ok.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public final class Security {

   private static final ThreadLocal<Boolean> PRIVILEGED = ThreadLocal.withInitial(() -> Boolean.FALSE);

   /*
    * We don't override initialValue because we don't want to allocate the ArrayDeque if we just want to check if a
    * Subject has been set.
    */
   private static final ThreadLocal<Deque<Subject>> SUBJECT = new InheritableThreadLocal<>() {
      @Override
      protected Deque<Subject> childValue(Deque<Subject> parentValue) {
         return parentValue == null ? null : new ArrayDeque<>(parentValue);
      }
   };

   private static boolean isTrustedClass(Class<?> klass) {
      // TODO: implement a better way
      String packageName = klass.getPackage().getName();
      return packageName.startsWith("org.infinispan") ||
            packageName.startsWith("org.jboss.as.clustering.infinispan");
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

   private static Deque<Subject> pre(Subject subject) {
      if (subject == null) {
         return null;
      }
      Deque<Subject> stack = SUBJECT.get();
      if (stack == null) {
         stack = new ArrayDeque<>(3);
         SUBJECT.set(stack);
      }
      stack.push(subject);
      return stack;
   }

   private static void post(Subject subject, Deque<Subject> stack) {
      if (subject != null) {
         stack.pop();
         if (stack.isEmpty()) {
            SUBJECT.remove();
         }
      }
   }

   public static void doAs(final Subject subject, final Runnable action) {
      Deque<Subject> stack = pre(subject);
      try {
         action.run();
      } finally {
         post(subject, stack);
      }
   }

   public static <T, R> R doAs(final Subject subject, Function<T, R> function, T t) {
      Deque<Subject> stack = pre(subject);
      try {
         return function.apply(t);
      } finally {
         post(subject, stack);
      }
   }

   public static <T, U, R> R doAs(final Subject subject, BiFunction<T, U, R> function, T t, U u) {
      Deque<Subject> stack = pre(subject);
      try {
         return function.apply(t, u);
      } finally {
         post(subject, stack);
      }
   }

   /**
    * A "lightweight" implementation of {@link Subject#doAs(Subject, PrivilegedAction)} which uses a ThreadLocal {@link
    * Subject} instead of modifying the current {@link AccessControlContext}.
    *
    * @see Subject#doAs(Subject, PrivilegedAction)
    */
   public static <T> T doAs(final Subject subject, final java.security.PrivilegedAction<T> action) {
      Deque<Subject> stack = pre(subject);
      try {
         return action.run();
      } finally {
         post(subject, stack);
      }
   }

   /**
    * A "lightweight" implementation of {@link Subject#doAs(Subject, PrivilegedExceptionAction)} which uses a
    * ThreadLocal {@link Subject} instead of modifying the current {@link AccessControlContext}.
    *
    * @see Subject#doAs(Subject, PrivilegedExceptionAction)
    */
   public static <T> T doAs(final Subject subject,
                            final java.security.PrivilegedExceptionAction<T> action)
         throws java.security.PrivilegedActionException {
      Deque<Subject> stack = pre(subject);
      try {
         return action.run();
      } catch (Exception e) {
         throw new PrivilegedActionException(e);
      } finally {
         post(subject, stack);
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
    * If using {@link Security#doAs(Subject, PrivilegedAction)} or {@link Security#doAs(Subject,
    * PrivilegedExceptionAction)}, returns the {@link Subject} associated with the current thread otherwise it returns
    * the {@link Subject} associated with the current {@link AccessControlContext}
    */
   public static Subject getSubject() {
      Deque<Subject> subjects = SUBJECT.get();
      if (subjects != null && !subjects.isEmpty()) {
         return subjects.peek();
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
    * Returns the first principal of a subject
    */
   public static Principal getSubjectUserPrincipal(Subject s) {
      if (s != null && !s.getPrincipals().isEmpty()) {
         return s.getPrincipals().iterator().next();
      }
      return null;
   }

   /**
    * A simplified version of Subject.toString() with the following advantages:
    * <ul>
    *    <li>only lists principals, ignoring credentials</li>
    *    <li>uses a compact, single-line format</li>
    *    <li>does not use synchronization</li>
    *    <li>does not use i18n messages</li>
    * </ul></uk>
    * @param subject
    * @return
    */
   public static String toString(Subject subject) {
      StringBuilder sb = new StringBuilder("Subject: [");
      boolean comma = false;
      for(Principal p : subject.getPrincipals()) {
         if (comma) {
            sb.append(" ,");
         }
         sb.append(p.toString());
         comma = true;
      }
      return sb.append(']').toString();
   }
}
