package org.infinispan.cdi;

import javax.inject.Qualifier;

import org.infinispan.cdi.util.defaultbean.DefaultBean;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * This annotation is used to qualify the provided default embedded cache configuration or/and the default embedded
 * cache manager.
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 * @deprecated The Infinispan CDI integration module now uses the {@link DefaultBean
 *             DefaultBean} from Solder. The OverrideDefault annotation is not necessary and will be removed in a future
 *             release. See {@link DefaultEmbeddedCacheManagerProducer} and {@link DefaultEmbeddedCacheConfigurationProducer}
 *             for more details.
 */
@Target({METHOD, FIELD, PARAMETER, TYPE})
@Retention(RUNTIME)
@Qualifier
@Documented
@Deprecated
public @interface OverrideDefault {
}
