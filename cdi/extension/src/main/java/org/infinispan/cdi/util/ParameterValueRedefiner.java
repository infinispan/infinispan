/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011, Red Hat, Inc. and/or its affiliates, and individual
 * contributors by the @authors tag. See the copyright.txt in the
 * distribution for a full listing of individual contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
