/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.config;

import java.lang.annotation.*;

/**
 * Represents an attribute of any XML element from a valid Infinispan configuration file.
 * <p>
 * 
 * Each ConfigurationAttribute should annotate the corresponding setter method in ancestor hierarchy
 * of the appropriate AbstractConfigurationBean.
 * <p>
 * 
 * ConfigurationAttribute should annotate the corresponding setter methods having one parameter that
 * could be either any primitive or java.lang.String.
 * 
 * 
 * @author Vladimir Blagojevic
 * @version $Id$
 */

@Retention(RetentionPolicy.RUNTIME)
@Target( { ElementType.METHOD })
public @interface ConfigurationAttribute {

   /**
    * Returns name of corresponding XML (ConfigurationElement) element that declares this attribute
    * 
    * @return
    */
   String containingElement();

   /**
    * Returns name of this attribute. Should match the corresponding attribute in XML
    * 
    * @return
    */
   String name();

   /**
    * Returns an array of String values representing allowed values for this attribute
    * 
    * @return
    */
   String [] allowedValues() default {};

   /**
    * Returns default value for this attribute
    * 
    * @return
    */
   String defaultValue() default "";

   /**
    * Returns description of this attribute
    * 
    * @return
    */
   String description() default "";

}
