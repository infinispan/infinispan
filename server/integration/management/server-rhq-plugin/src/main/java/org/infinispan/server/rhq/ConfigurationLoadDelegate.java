/*
 * RHQ Management Platform
 * Copyright (C) 2005-2011 Red Hat, Inc.
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation version 2 of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
 */
package org.infinispan.server.rhq;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.configuration.Property;
import org.rhq.core.domain.configuration.PropertyList;
import org.rhq.core.domain.configuration.PropertyMap;
import org.rhq.core.domain.configuration.PropertySimple;
import org.rhq.core.domain.configuration.definition.ConfigurationDefinition;
import org.rhq.core.domain.configuration.definition.PropertyDefinition;
import org.rhq.core.domain.configuration.definition.PropertyDefinitionList;
import org.rhq.core.domain.configuration.definition.PropertyDefinitionMap;
import org.rhq.core.domain.configuration.definition.PropertyDefinitionSimple;
import org.rhq.core.domain.configuration.definition.PropertyGroupDefinition;
import org.rhq.core.pluginapi.configuration.ConfigurationFacet;
import org.rhq.core.pluginapi.configuration.ConfigurationUpdateReport;
import org.rhq.modules.plugins.jbossas7.ASConnection;
import org.rhq.modules.plugins.jbossas7.json.Address;
import org.rhq.modules.plugins.jbossas7.json.Operation;
import org.rhq.modules.plugins.jbossas7.json.ReadAttribute;
import org.rhq.modules.plugins.jbossas7.json.ReadChildrenResources;
import org.rhq.modules.plugins.jbossas7.json.ReadResource;
import org.rhq.modules.plugins.jbossas7.json.Result;

public class ConfigurationLoadDelegate implements ConfigurationFacet {

    final Log log = LogFactory.getLog(this.getClass());

    private Address address;
    private ASConnection connection;
    private ConfigurationDefinition configurationDefinition;
    private String nameFromPathProperty;
    private boolean includeRuntime;

    /**
     * Create a new configuration delegate, that reads the attributes for the resource at address.
     * @param configDef Configuration definition for the configuration
     * @param connection asConnection to use
     * @param address address of the resource.
     */
    public ConfigurationLoadDelegate(ConfigurationDefinition configDef, ASConnection connection, Address address) {
        this(configDef, connection, address, false);
    }

    /**
     * Create a new configuration delegate, that reads the attributes for the resource at address.
     * @param configDef Configuration definition for the configuration
     * @param connection asConnection to use
     * @param address address of the resource.
     */
    public ConfigurationLoadDelegate(ConfigurationDefinition configDef, ASConnection connection, Address address,
        boolean includeRuntime) {
        this.configurationDefinition = configDef;
        this.connection = connection;
        this.address = address;
        this.includeRuntime = includeRuntime;
    }

    /**
     * Trigger loading of a configuration by talking to the remote resource.
     * @return The initialized configuration
     * @throws Exception If anything goes wrong.
     */
    public Configuration loadResourceConfiguration() throws Exception {

        Configuration config = new Configuration();

        /*
         * Grouped definitions get a special treatment, as they may have a special property
         * that will be evaluated to look at a child resource or a special attribute or such
         */
        List<PropertyGroupDefinition> gdef = configurationDefinition.getGroupDefinitions();
        for (PropertyGroupDefinition pgDef : gdef) {
            loadHandleGroup(config, pgDef);
        }
        /*
         * Now handle the non-grouped properties
         */
        List<PropertyDefinition> nonGroupdedDefs = configurationDefinition.getNonGroupedProperties();
        Operation op = new ReadResource(address);
        if (includeRuntime) {
            op.addAdditionalProperty("include-runtime", "true"); // Include runtime values for attributes
        }
        //        op.addAdditionalProperty("recursive", "true"); // Also get sub-resources
        loadHandleProperties(config, nonGroupdedDefs, op);

        return config;
    }

    /**
     * Handle a set of grouped properties. The name of the group tells us how to deal with it:
     * <ul>
     *     <li>attribute: read the passed attribute of the resource</li>
     *     <li>children:  read the children of the given child-type</li>
     * </ul>
     * @param config Configuration to return
     * @param groupDefinition Definition of this group
     * @throws Exception If anything goes wrong
     */
    private void loadHandleGroup(Configuration config, PropertyGroupDefinition groupDefinition) throws Exception {
        Operation operation = null;
        String groupName = groupDefinition.getName();
        if (groupName.startsWith("attribute:")) {
            String attr = groupName.substring("attribute:".length());
            operation = new ReadAttribute(address, attr);
        } else if (groupName.startsWith("children:")) {
            String type = groupName.substring("children:".length());
            if (type.contains(":")) {

                // If the third part ends with a ?, we fill this config prop with the resource name from the path
                String tmp = type.substring(type.indexOf(":") + 1);
                if (tmp.endsWith("+-")) {
                    nameFromPathProperty = tmp.substring(0, tmp.length() - 2);
                }

                // We need the type for reading
                type = type.substring(0, type.indexOf(":"));

            }
            operation = new ReadChildrenResources(address, type);
            operation.addAdditionalProperty("recursive", "true");
        } else if (groupName.startsWith("child:")) {
            String subPath = groupName.substring("child:".length());
            if (!subPath.contains("="))
                throw new IllegalArgumentException("subPath of 'child:' expression has no =");

            if (subPath.contains(":")) {
                subPath = subPath.substring(0,subPath.indexOf(':'));
            }
            boolean includeRuntime=false;
            if (subPath.endsWith("*")) {
                subPath = subPath.substring(0,subPath.length()-1);
                includeRuntime=true;
            }

            Address address1 = new Address(address);
            address1.addSegment(subPath);
            operation = new ReadResource(address1);
            if (includeRuntime)
                ((ReadResource)operation).includeRuntime(true);
        } else {//no special handling of <c:groups> details required.
            //Just assume normal group operations aggregation semantics, so retrieve entries and load.
            ReadResource op = new ReadResource(address);
// Added these lines to allow for runtime attributes to be returned
            if (includeRuntime) {
               op.includeRuntime(true);
            }
// Added these lines to allow for runtime attributes to be returned
            List<PropertyDefinition> listedDefs = configurationDefinition.getPropertiesInGroup(groupName);
            loadHandleProperties(config, listedDefs, op);
            return;
        }
        List<PropertyDefinition> listedDefs = configurationDefinition.getPropertiesInGroup(groupName);
        loadHandleProperties(config, listedDefs, operation);

    }

    private void loadHandleProperties(Configuration config, List<PropertyDefinition> definitions, Operation op)
        throws Exception {
        if (definitions.size() == 0)
            return;

        Result operationResult = connection.execute(op);
        if (!operationResult.isSuccess()) {
            // check if the definition is needed -- required==true or not. Only complain for required ones
            if (definitions.size()==1 && definitions.get(0).isRequired()==false) {
                return;
            }

            throw new IOException("Operation " + op + " failed: " + operationResult.getFailureDescription());
        }

        if (operationResult.getResult() instanceof List) {
            PropertyList propertyList = loadHandlePropertyList((PropertyDefinitionList) definitions.get(0),
                operationResult.getResult());

            if (propertyList != null)
                config.put(propertyList);
            return;
        }

        Map<String, Object> results = (Map<String, Object>) operationResult.getResult();

        for (PropertyDefinition propDef : definitions) {

            /*
             * We may have a mismatch for groups where the <c:group name="children:*"> child is a list of maps
             * but the result is actually the contained map
             */
            String propertyName = propDef.getName();
            if (propDef instanceof PropertyDefinitionList && (propertyName.startsWith("*"))) {
                propDef = ((PropertyDefinitionList) propDef).getMemberDefinition();

                if (!(propDef instanceof PropertyDefinitionMap)) {
                    log.error("Embedded child is not a map");
                    return;
                }
                // Now we are at map level which matches the operations results

                PropertyList list = new PropertyList(propertyName);
                if (results != null) {
                    for (Map.Entry<String, Object> entry : results.entrySet()) {
                        Object val = entry.getValue();
                        String key = entry.getKey();

                        PropertyMap propertyMap = loadHandlePropertyMap((PropertyDefinitionMap) propDef, val, key);

                        if (nameFromPathProperty != null) {
                            // We need to fill that property as well
                            PropertySimple ps = new PropertySimple(nameFromPathProperty, key);
                            propertyMap.put(ps);
                        }

                        if (propertyMap != null)
                            list.add(propertyMap);
                    }
                }


                config.put(list);
            } else if (propDef instanceof PropertyDefinitionMap && (propertyName.startsWith("*"))) {
                // a map in a group with name child:type=name
                Map<String, Object> mapResult = results;
                PropertyMap propertyMap = loadHandlePropertyMap((PropertyDefinitionMap) propDef, mapResult,
                    propertyName);
                config.put(propertyMap);

            } else { // standard case

                Object valueObject;

                //strip :number from properties
                if (propertyName.contains(":")) {
                    try {
                        Integer.parseInt(propertyName.substring(propertyName.lastIndexOf(':') + 1));
                        propertyName = propertyName.substring(0, propertyName.lastIndexOf(':'));
                    } catch (Exception e) {
                        //do nothing, this means the property name does not end with :number, so nothing needs to be stripped
                    }
                }

                if (propertyName.endsWith(":expr") || propertyName.endsWith(":collapsed")
                    || propertyName.endsWith(":nullable")) {
                    String realName = propertyName.substring(0, propertyName.indexOf(":"));
                    valueObject = results.get(realName);
                } else {
                    valueObject = results.get(propertyName);
                }

                if (propDef instanceof PropertyDefinitionSimple) {

                    PropertySimple value = loadHandlePropertySimple((PropertyDefinitionSimple) propDef, valueObject);
                    if (value != null)
                        config.put(value);
                }

                else if (propDef instanceof PropertyDefinitionList) {
                    PropertyList propertyList = loadHandlePropertyList((PropertyDefinitionList) propDef, valueObject);

                    if (propertyList != null)
                        config.put(propertyList);
                } else if (propDef instanceof PropertyDefinitionMap) {
                    PropertyMap propertyMap = loadHandlePropertyMap((PropertyDefinitionMap) propDef, valueObject, null);

                    if (propertyMap != null)
                        config.put(propertyMap);
                }
            }
        }
    }

    PropertySimple loadHandlePropertySimple(PropertyDefinitionSimple propDef, Object valueObject) {
        PropertySimple propertySimple;

        String name = propDef.getName();
        if (valueObject != null) {
            // Property is non-null -> return it.

            if (valueObject instanceof Map) { // If this is a map and no single type, get the EXPRESSION_VALUE
                Object o = ((Map) valueObject).get("EXPRESSION_VALUE");
                propertySimple = new PropertySimple(name, o);
            } else {
                propertySimple = new PropertySimple(name, valueObject);
            }
        } else {
            // property is null? Check if it is required
            if (propDef.isRequired()) {
                String defaultValue = ((PropertyDefinitionSimple) propDef).getDefaultValue();
                propertySimple = new PropertySimple(name, defaultValue);
            } else { // Not required and null -> return null
                propertySimple = new PropertySimple(name, null);
            }
        }
        return propertySimple;

    }

    /**
     * Handle a Map of ...
     *
     * @param propDef Definition of the map
     * @param valueObject the objects to put into the map
     * @param optionalEntryName
     * @return the populated map
     */
    PropertyMap loadHandlePropertyMap(PropertyDefinitionMap propDefMap, Object valueObject, String optionalEntryName) {
        if (valueObject == null)
            return null;

        String propDefName = propDefMap.getName();
        PropertyMap propertyMap = new PropertyMap(propDefName);
        String specialNameProp = null;
        if (propDefName.startsWith("*:")) {
            specialNameProp = propDefName.substring(2);
            PropertySimple additionalNameProperty = new PropertySimple(specialNameProp, optionalEntryName);
            propertyMap.put(additionalNameProperty);
        }

        List<PropertyDefinition> propDefs = propDefMap.getPropertyDefinitions();

        if (propDefName.endsWith(":collapsed")) {
            // The result is a map of {" a" : " b" }, while the propdef is in the form
            // map { prop {name=a } , prop {name=b) }
            if (propDefs.size()<1 || propDefs.size() > 2)
                throw new IllegalArgumentException("Collapsed map [" + propDefName + "] needs 1 or 2 entries and not "
                    + propDefs.size());

            Iterator<PropertyDefinition> iterator = propDefs.iterator();
            String name = iterator.next().getName();
            Map<String, String> objects = (Map<String, String>) valueObject;
            String val = objects.keySet().iterator().next();
            PropertySimple ps = new PropertySimple(name, val);
            propertyMap.put(ps);
            if (iterator.hasNext()) {
                name = iterator.next().getName();
                val = objects.values().iterator().next();
                ps = new PropertySimple(name, val);
                propertyMap.put(ps);
            }

            return propertyMap;
        }

        Map<String, Object> objects = (Map<String, Object>) valueObject;
        for (PropertyDefinition propDef : propDefs) {
            String key = propDef.getName();
            if (key.equals(specialNameProp)) // Skip over specialName prop, as we have processed that already.
                continue;

            // special case: if the key is "*", we just pick the first element
            Object o;
            if (key.equals("*")) {
                o = objects.entrySet().iterator().next().getValue();
            } else if (key.endsWith(":expr")) {
                // TODO we need to check te
                String tmp = key.substring(0, key.indexOf(":"));
                o = objects.get(tmp);
            } else {
                o = objects.get(key);
            }

            Property property;
            if (propDef instanceof PropertyDefinitionSimple)
                property = loadHandlePropertySimple((PropertyDefinitionSimple) propDef, o);
            else if (propDef instanceof PropertyDefinitionList)
                property = loadHandlePropertyList((PropertyDefinitionList) propDef, o);
            else if (propDef instanceof PropertyDefinitionMap)
                property = loadHandlePropertyMap((PropertyDefinitionMap) propDef, o, null);
            else
                throw new IllegalArgumentException("Unknown property type in map property [" + propDefName + "]");

            if (property != null)
                propertyMap.put(property);
            else {
                if (log.isDebugEnabled())
                    log.debug("Property " + key + " was null");
            }

        }

        return propertyMap;
    }

    /**
     * Handle a List of ...
     * @param propDef Definition of this list
     * @param valueObject The objects to put into the list
     * @return the property that describes the list.
     */
    PropertyList loadHandlePropertyList(PropertyDefinitionList propDef, Object valueObject) {
        String propertyName = propDef.getName();
        PropertyList propertyList = new PropertyList(propertyName);
        PropertyDefinition memberDefinition = propDef.getMemberDefinition();
        if (memberDefinition == null)
            throw new IllegalArgumentException("Member definition for property [" + propertyName + "] was null");

        if (valueObject == null) {
            //            System.out.println("vo null");
            return null;
        }

        Collection<Object> objects;
        if (valueObject instanceof List)
            objects = (List<Object>) valueObject;
        else /*if (valueObject instanceof Map)*/{
            objects = ((Map) valueObject).values();
        }

        if (memberDefinition instanceof PropertyDefinitionSimple) {
            for (Object obj : objects) {
                PropertySimple property = loadHandlePropertySimple((PropertyDefinitionSimple) memberDefinition, obj);
                if (property != null)
                    propertyList.add(property);
            }
        } else if (memberDefinition instanceof PropertyDefinitionMap) {
            for (Object obj : objects) {
                Map<String, Object> map = (Map<String, Object>) obj;

                PropertyMap propertyMap = loadHandlePropertyMap((PropertyDefinitionMap) propDef.getMemberDefinition(),
                    map, null);
                if (propertyMap != null)
                    propertyList.add(propertyMap);
            }
        }
        // TODO List of lists ?
        return propertyList;
    }

    /**
     * Write the configuration back to the AS. Care must be taken, not to send properties that
     * are read-only, as AS will choke on them.
     * @param report
     */
    public void updateResourceConfiguration(ConfigurationUpdateReport report) {

        throw new IllegalArgumentException("Please use ConfigurationWriteDelegate");

    }
}