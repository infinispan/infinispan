package org.infinispan.factories.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.infinispan.factories.AbstractComponentFactory;
import org.infinispan.factories.ComponentRegistry;


/**
 * Used to annotate a method as one that is used to inject a registered component into another component.  The component
 * to be constructed must be built using the {@link AbstractComponentFactory#construct(Class)} method, or if your object
 * that needs components injected into it already exists, it can be built using the {@link
 * ComponentRegistry#wireDependencies(Object)} method.
 * <p/>
 * Usage example:
 * <pre>
 *       public class MyClass
 *       {
 *          private TransactionManager tm;
 *          private BuddyManager bm;
 *          private Notifier n;
 * <p/>
 *          &amp;Inject
 *          public void setTransactionManager(TransactionManager tm)
 *          {
 *             this.tm = tm;
 *          }
 * <p/>
 *          &amp;Inject
 *          public void injectMoreStuff(BuddyManager bm, Notifier n)
 *          {
 *             this.bm = bm;
 *             this.n = n;
 *          }
 *       }
 * <p/>
 * </pre>
 * and an instance of this class can be constructed and wired using
 * <pre>
 *       MyClass myClass = componentFactory.construct(MyClass.class); // instance will have dependencies injected.
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
@Retention(RetentionPolicy.RUNTIME)

// only applies to fields.
@Target({ ElementType.METHOD, ElementType.FIELD })
public @interface Inject {
}
