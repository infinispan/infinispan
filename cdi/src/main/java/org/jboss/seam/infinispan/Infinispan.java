/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010, Red Hat, Inc., and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
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
package org.jboss.seam.infinispan;

import org.infinispan.config.Configuration;
import org.infinispan.manager.CacheContainer;
import org.jboss.seam.solder.bean.generic.GenericType;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Configure an Infinispan Cache. By default, Seam will use the
 * {@link CacheContainer}
 *
 * @author Pete Muir
 *
 */

@Retention(RUNTIME)
@Target({ METHOD, FIELD, PARAMETER, TYPE })
@GenericType(Configuration.class)
public @interface Infinispan {

   /**
    * The name of the cache. If no name is specified the default cache will be
    * used.
    */
   String value() default "";

}
