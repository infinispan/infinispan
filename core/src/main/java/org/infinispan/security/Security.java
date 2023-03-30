package org.infinispan.security;

import java.security.Principal;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.security.auth.Subject;

import org.infinispan.commons.jdkspecific.CallerId;

/**
 * Security. A simple class to implement caller privileges without a security manager.
 * <p>
 * N.B. this uses the caller's {@link Package}, this can easily be subverted by placing the calling code within the
 * org.infinispan hierarchy. For most purposes, however, this is ok.
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

   public static <T> T doPrivileged(Supplier<T> action) {
      if (!isPrivileged() && isTrustedClass(CallerId.getCallerClass(3))) {
         try {
            PRIVILEGED.set(true);
            return action.get();
         } finally {
            PRIVILEGED.remove();
         }
      } else {
         return action.get();
      }
   }

   public static void doPrivileged(Runnable action) {
      if (!isPrivileged() && isTrustedClass(CallerId.getCallerClass(3))) {
         try {
            PRIVILEGED.set(true);
            action.run();
         } finally {
            PRIVILEGED.remove();
         }
      } else {
         action.run();
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

   public static <T> T doAs(final Subject subject, final Supplier<T> action) {
      Deque<Subject> stack = pre(subject);
      try {
         return action.get();
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

   public static boolean isPrivileged() {
      return PRIVILEGED.get();
   }

   /**
    * If using {@link Security#doAs(Subject, Runnable)} or {@link Security#doAs(Subject, Function, Object)} or {@link  Security#doAs(Subject, BiFunction, Object, Object)},
    * returns the {@link Subject} associated with the current thread otherwise it returns
    * null.
    */
   public static Subject getSubject() {
      Deque<Subject> subjects = SUBJECT.get();
      if (subjects != null && !subjects.isEmpty()) {
         return subjects.peek();
      } else {
         return null;
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

   public static String getSubjectUserPrincipalName(Subject s) {
      if (s != null && !s.getPrincipals().isEmpty()) {
         return s.getPrincipals().iterator().next().getName();
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
