package org.infinispan.cdi.util.defaultbean;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import javax.enterprise.inject.Alternative;
import javax.enterprise.inject.spi.AfterBeanDiscovery;

/**
 * <p>
 * Annotation that signifies that a bean should only be registered if no other
 * instance with the same type and qualifiers is registered. The bean only has
 * the type specified in the <code>value</code> attribute and {@link Object}.
 * </p>
 * <p/>
 * <p>
 * Managed beans, producer methods and producer fields can all be made into
 * default beans.
 * </p>
 * <p/>
 * <p>
 * If a managed bean is declared to be a default bean then all producers methods
 * and fields on the bean are also considered to be default beans. In this case
 * if the <code>&#64;DefaultBean</code> annotation is not explicitly specified
 * then the default bean type is considered to be the type returned by
 * {@link Method#getGenericReturnType} or {@link Field#getGenericType()} for a
 * field.
 * </p>
 * <p/>
 * <p>
 * In some ways this is similar to the functionality provided by &#64;
 * {@link Alternative} however there are some important distinctions
 * </p>
 * <ul>
 * <li>No XML is required, if an alternative implementation is available it is
 * used automatically</li>
 * <li>The bean is registered across all modules, not on a per module basis</li>
 * </ul>
 * <p/>
 * <p>
 * It is also important to note that beans registered in the
 * {@link AfterBeanDiscovery} event may not be picked up as default beans.
 * </p>
 *
 * @author Stuart Douglas
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.FIELD})
@Documented
public @interface DefaultBean {
    /**
     * <p>
     * The type of the bean. If another bean is found with this type and the same
     * qualifiers this bean will not be installed.
     * </p>
     * <p/>
     * <p>
     * This bean will only be installed with the type specified here
     * </p>
     */
    public Class<?> value();
}
