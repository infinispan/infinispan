package org.infinispan.cdi.util;

import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.InjectionPoint;

/**
 * Provides the ability to redefine the value of a parameter on an
 * {@link InjectableMethod} via the
 * {@link #redefineParameterValue(ParameterValue)} callback.
 *
 * @author Pete Muir
 * @see InjectableMethod
 */
public interface ParameterValueRedefiner {

    /**
     * Provides the default parameter's value, along with metadata about the
     * parameter to a parameter redefinition.
     *
     * @author Pete Muir
     * @see ParameterValueRedefiner
     * @see InjectableMethod
     */
    public static class ParameterValue {

        private final int position;
        private final InjectionPoint injectionPoint;
        private final BeanManager beanManager;

        ParameterValue(int position, InjectionPoint injectionPoint, BeanManager beanManager) {
            this.position = position;
            this.injectionPoint = injectionPoint;
            this.beanManager = beanManager;
        }

        /**
         * Get the position of the parameter in the member's parameter list.
         *
         * @return the position of the parameter
         */
        public int getPosition() {
            return position;
        }

        /**
         * Get the {@link InjectionPoint} for the parameter.
         *
         * @return the injection point
         */
        public InjectionPoint getInjectionPoint() {
            return injectionPoint;
        }

        /**
         * Get the default value of the parameter. The default value is that which
         * would be injected according to the CDI type safe resolution rules.
         *
         * @param creationalContext the creationalContext to use to obtain the
         *                          injectable reference.
         * @return the default value
         */
        public Object getDefaultValue(CreationalContext<?> creationalContext) {
            return beanManager.getInjectableReference(injectionPoint, creationalContext);
        }

    }

    /**
     * Callback allowing the default parameter value (that which would be
     * injected according to the CDI type safe resolution rules) to be
     * overridden.
     *
     * @param value the default value
     * @return the overridden value
     */
    public Object redefineParameterValue(ParameterValue value);

}
