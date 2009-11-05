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

import java.io.IOException;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import org.infinispan.config.Configuration.ModuleConfigurationBean;
import org.infinispan.config.Configuration.ModulesExtensionType;
import org.infinispan.util.ModuleProperties;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * ModuleConfigurationResolverVisitor resolves module's configuration class and unmarshalls it from XML.
 * 
 * 
 * @author Vladimir Blagojevic
 * @since 4.0
 */
public class ModuleConfigurationResolverVisitor extends AbstractConfigurationBeanVisitor {

    Document root;

    public ModuleConfigurationResolverVisitor(Document d) {
        if (d == null)
            throw new IllegalArgumentException("Cannot use null document");
        this.root = d;
    }

    @Override
    public void visitModulesExtentionsType(ModulesExtensionType bean) {
        List<ModuleConfigurationBean> list = bean.getModuleConfigs();
        for (ModuleConfigurationBean module : list) {
            try {
                ModuleProperties props = ModuleProperties.loadModuleProperties(module.getName());
                if (props != null) {
                    Class<AbstractConfigurationBean> configurationClass = module.resolveConfigurationClass(props.getConfigurationClassName());
                    NodeList nodeList = root.getElementsByTagName(Configuration.ELEMENT_MODULE_NAME);
                    
                    findModuleInXML:
                    for (int i = nodeList.getLength() - 1; i >= 0; i--) {
                        Element node = (Element) nodeList.item(i);
                        String name = node.getAttribute(Configuration.MODULE_IDENTIFIER);
                        if (name.equals(module.getName())) {
                            NodeList childNodes = node.getChildNodes();
                            for (int j = 0; j < childNodes.getLength(); j++) {
                                Node item = childNodes.item(j);
                                //find first child element
                                if (item.getNodeType() == Node.ELEMENT_NODE) {
                                    AbstractConfigurationBean configBean = loadConfigurationBeanModule((Element) item, configurationClass);
                                    module.setConfigurationBean(configBean);
                                    break findModuleInXML;
                                }
                            }
                        }
                    }
                }
            } catch (Exception e) {
                throw new ConfigurationException("Could not load configuration bean for module "
                                + module.getName(), e);
            }
        }
    }  
    
    private AbstractConfigurationBean loadConfigurationBeanModule(Element node,
                    Class<AbstractConfigurationBean> configurationClass) throws IOException {
        try {
            JAXBContext jc = JAXBContext.newInstance(configurationClass);
            Unmarshaller u = jc.createUnmarshaller();
            u.setSchema(null);
            return (AbstractConfigurationBean) u.unmarshal(node);
        } catch (ConfigurationException cex) {
            throw cex;
        } catch (NullPointerException npe) {
            throw npe;
        } catch (Exception e) {
            IOException ioe = new IOException(e.getLocalizedMessage());
            ioe.initCause(e);
            throw ioe;
        }
    }
}
