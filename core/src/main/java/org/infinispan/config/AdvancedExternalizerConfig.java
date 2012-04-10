/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
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

import org.infinispan.marshall.AdvancedExternalizer;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlTransient;

/**
 * Defines custom Externalizers to be registered with marshalling framework
 *
 * @author Vladimir Blagojevic
 * @since 5.0
 */
@XmlAccessorType(XmlAccessType.PROPERTY)
@ConfigurationDoc(name = "advancedExternalizer")
public class AdvancedExternalizerConfig extends AbstractConfigurationBeanWithGCR {

   /** The serialVersionUID */
   private static final long serialVersionUID = -5161505617995274887L;

   @ConfigurationDocRef(bean = AdvancedExternalizerConfig.class, targetElement = "setExternalizerClass")
   protected String externalizerClass;

   private AdvancedExternalizer<?> advancedExternalizer;

   @ConfigurationDocRef(bean = AdvancedExternalizerConfig.class, targetElement = "setId")
   protected Integer id;

   public String getExternalizerClass() {
      if (externalizerClass == null && advancedExternalizer != null)
         externalizerClass = advancedExternalizer.getClass().getName();

      return externalizerClass;
   }

   /**
    * Fully qualified class name of an {@link org.infinispan.marshall.AdvancedExternalizer}
    * implementation that knows how to marshall or unmarshall instances of one, or
    * several, user-defined, types.
    * 
    * @param externalizerClass
    */
   @XmlAttribute
   public AdvancedExternalizerConfig setExternalizerClass(String externalizerClass) {
      this.externalizerClass = externalizerClass;
      return this;
   }

   public Integer getId() {
      if (id == null && advancedExternalizer != null)
         id = advancedExternalizer.getId();
      return id;
   }

   /**
    * This identifier distinguishes between different user-defined {@link org.infinispan.marshall.AdvancedExternalizer}
    * implementations, providing a more performant way to ship class information around
    * rather than passing class names or class information in general around.
    *
    * Only positive ids are allowed, and you can use any number as long as it does not
    * clash with an already existing number for a {@link org.infinispan.marshall.AdvancedExternalizer} implementation.
    *
    * If there're any clashes, Infinispan will abort startup and will provide class
    * information of the ids clashing.
    * 
    * @param id
    */
   @XmlAttribute
   public AdvancedExternalizerConfig setId(Integer id) {
      this.id = id;
      return this;
   }

   @XmlTransient // Prevent JAXB from thinking that advancedExternalizer is an XML attribute
   public AdvancedExternalizer<?> getAdvancedExternalizer() {
      return advancedExternalizer;
   }

   public AdvancedExternalizerConfig setAdvancedExternalizer(AdvancedExternalizer<?> advancedExternalizer) {
      this.advancedExternalizer = advancedExternalizer;
      return this;
   }

   public String toString() {
      return "AdvancedExternalizerConfig{";
   }

   public boolean equals(Object o) {
      if (this == o)
         return true;
      if (!(o instanceof AdvancedExternalizerConfig))
         return false;

      AdvancedExternalizerConfig that = (AdvancedExternalizerConfig) o;
      if (externalizerClass != null && !externalizerClass.equals(that.externalizerClass))
         return false;
      if (id != null && !id.equals(that.id))
         return false;

      return true;
   }

   public int hashCode() {
      int result;
      result = (externalizerClass != null ? externalizerClass.hashCode() : 0);
      result = 31 * result + (id != null ? id.hashCode() : 0);
      return result;
   }

   public void accept(ConfigurationBeanVisitor v) {
      v.visitAdvancedExternalizerConfig(this);
   }
}
