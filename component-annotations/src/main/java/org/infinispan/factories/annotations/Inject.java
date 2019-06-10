package org.infinispan.factories.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Used to annotate a method or field as an injection point.
 *
 * <p>The method or field must not be {@code private}, usually it's best to have it package-private.</p>
 *
 * <p>Usage example:</p>
 * <pre>
 *       public class MyClass
 *       {
 *          private TransactionManager tm;
 *          private BuddyManager bm;
 *          private Notifier n;
 *
 *          &amp;Inject
 *          public void setTransactionManager(TransactionManager tm)
 *          {
 *             this.tm = tm;
 *          }
 *
 *          &amp;Inject
 *          public void injectMoreStuff(BuddyManager bm, Notifier n)
 *          {
 *             this.bm = bm;
 *             this.n = n;
 *          }
 *       }
 * </pre>
 * and an instance of this class can be constructed and wired using
 * <pre>
 *       MyClass myClass = componentRegistry.getComponent(MyClass.class);
 * </pre>
 *
 * Methods annotated with this Inject annotation should *only* set class fields. They should do nothing else.
 * If you need to do some work to prepare the component for use, do it in a {@link Start} method since this is only
 * called once when a component starts.
 *
 * @author Manik Surtani
 * @since 4.0
 */
// ensure this annotation is available at runtime.
@Retention(RetentionPolicy.CLASS)
@Target({ ElementType.METHOD, ElementType.FIELD })
public @interface Inject {
}
