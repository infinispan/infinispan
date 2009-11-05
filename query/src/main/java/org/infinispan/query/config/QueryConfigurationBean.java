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
package org.infinispan.query.config;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

import org.infinispan.config.AbstractConfigurationBean;

@XmlRootElement(name = "indexing")
@XmlAccessorType(XmlAccessType.PROPERTY)
public class QueryConfigurationBean extends AbstractConfigurationBean {

    /** The serialVersionUID */
    private static final long serialVersionUID = 2891683014353342549L;

    protected Boolean enabled;

    protected Boolean indexLocalOnly;

    public Boolean isEnabled() {
        return enabled;
    }

    @XmlAttribute
    public void setEnabled(Boolean enabled) {
        testImmutability("enabled");
        this.enabled = enabled;
    }

    public Boolean isIndexLocalOnly() {
        return indexLocalOnly;
    }

    @XmlAttribute
    public void setIndexLocalOnly(Boolean indexLocalOnly) {
        testImmutability("indexLocalOnly");
        this.indexLocalOnly = indexLocalOnly;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof QueryConfigurationBean))
            return false;

        QueryConfigurationBean that = (QueryConfigurationBean) o;

        if (enabled != null ? !enabled.equals(that.enabled) : that.enabled != null)
            return false;

        if (indexLocalOnly != null ? !indexLocalOnly.equals(that.indexLocalOnly): that.indexLocalOnly != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = enabled != null ? enabled.hashCode() : 0;
        result = 31 * result + (indexLocalOnly != null ? indexLocalOnly.hashCode() : 0);
        return result;
    }

    @Override
    protected boolean hasComponentStarted() {
        return false;
    }
}
