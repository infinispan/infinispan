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

import java.lang.reflect.Method;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * AbstractConfigurationBeanVisitor is a convenience super class for ConfigurationBeanVisitor
 * classes.
 * 
 * <p>
 * 
 * Subclasses of AbstractConfigurationBeanVisitor should define the most parameter type specific
 * definitions of <code>void visit(AbstractConfigurationBean bean); </code> method. These methods
 * are going to be invoked by traverser as it comes across these types during traversal of
 * <code>InfinispanConfiguration</code> tree.
 * 
 * <p>
 * 
 * For example, method <code>public void visit(SingletonStoreConfig ssc)</code> defined in a
 * subclass of this class is going to be invoked as the traverser comes across instance(s) of
 * SingletonStoreConfig.
 * 
 * @author Vladimir Blagojevic
 * @since 4.0
 */
public abstract class AbstractConfigurationBeanVisitor implements ConfigurationBeanVisitor {

    protected transient Log log = LogFactory.getLog(getClass());

    private Method findVisitMethod(AbstractConfigurationBean bean) throws Exception {
        Class<?> cl = bean.getClass();
        while (!cl.equals(AbstractConfigurationBean.class)) {
            try {
                return this.getClass().getDeclaredMethod("visit", new Class[] { cl });
            } catch (NoSuchMethodException ex) {
                cl = cl.getSuperclass();
            }
        }
        // Check through interfaces for matching method
        Class<?>[] interfaces = bean.getClass().getInterfaces();
        for (int i = 0; i < interfaces.length; i++) {
            try {
                return this.getClass().getDeclaredMethod("visit", new Class[] { interfaces[i] });
            } catch (NoSuchMethodException ex) {
            }
        }
        return null;
    }

    public void visit(AbstractConfigurationBean bean) {
        Method m = null;
        try {
            m = findVisitMethod(bean);
        } catch (Exception e) {
            log.warn("Could not reflect visit method for bean " + bean, e);
        }
        if (m == null) {
            defaultVisit(bean);
        } else {
            try {
                m.invoke(this, new Object[] { bean });
            } catch (Exception e) {
                log.warn("Invocation for visitor method " + m + " on bean " + bean
                                + " has thrown exception", e);               
            }
        }
    }

    public void defaultVisit(AbstractConfigurationBean c) {}

}
