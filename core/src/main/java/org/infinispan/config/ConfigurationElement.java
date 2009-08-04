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
import org.infinispan.config.parsing.ConfigurationElementReader;
import org.infinispan.config.parsing.ConfigurationElementWriter;

/**
 * Represents XML element from a valid Infinispan configuration file.
 * <p>
 * 
 * Each ConfigurationElement should annotate the most derived subclass of AbstractConfigurationBean
 * that contains setter methods for XML attributes of the corresponding XML element (the one that
 * ConfigurationElement represents)
 * 
 * <p>
 * For example, CacheLoaderManagerConfig is annotated with
 * <code>@ConfigurationElement(name="loaders",parent="default")</code> annotation since
 * CacheLoaderManagerConfig is the most derived subclass of AbstractConfigurationBean that contains
 * setter methods for attributes contained in <code><loaders></code> XML element.
 * 
 * @see GlobalConfiguration
 * @see Configuration
 * @see CacheLoaderManagerConfig
 * @author Vladimir Blagojevic
 */

@Retention(RetentionPolicy.RUNTIME)
@Target( { ElementType.TYPE })
public @interface ConfigurationElement {

   public enum Cardinality {
      ONE, UNBOUNDED
   };

   /**
    * Returns name of corresponding XML element
    * 
    * @return
    */
   String name();

   /**
    * Returns name of corresponding parent XML element.
    * 
    * @return
    */
   String parent();

   /**
    * Returns Cardinality.ONE if parent ConfigurationElement can have zero or one child defined by
    * this ConfigurationElement. In case parent can have multiple ConfigurationElement with the same
    * name returns Cardinality.UNBOUNDED
    * 
    * @return
    */
   Cardinality cardinalityInParent() default Cardinality.ONE;

   /**
    * Returns description of this element
    * 
    * @return
    */
   String description() default "";

   /**
    * Returns class of custom parser needed to process this ConfigurationElement
    * 
    * @return
    */
   Class<? extends ConfigurationElementReader> customReader() default ConfigurationElementReader.class;

   /**
    * Returns class of custom writer for this ConfigurationElement
    * 
    * @return
    */
   Class<? extends ConfigurationElementWriter> customWriter() default ConfigurationElementWriter.class;

}
