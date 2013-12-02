/*
 * RHQ Management Platform
 * Copyright (C) 2005-2012 Red Hat, Inc.
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

import java.util.*;

import org.rhq.core.domain.configuration.*;
import org.rhq.core.domain.measurement.MeasurementDataTrait;
import org.rhq.core.domain.measurement.MeasurementReport;
import org.rhq.core.domain.measurement.MeasurementScheduleRequest;
import org.rhq.core.domain.resource.CreateResourceStatus;
import org.rhq.core.pluginapi.configuration.ConfigurationFacet;
import org.rhq.core.pluginapi.configuration.ConfigurationUpdateReport;
import org.rhq.core.pluginapi.inventory.CreateChildResourceFacet;
import org.rhq.core.pluginapi.inventory.CreateResourceReport;
import org.rhq.modules.plugins.jbossas7.json.Address;
import org.rhq.modules.plugins.jbossas7.json.Operation;
import org.rhq.modules.plugins.jbossas7.json.Result;

/**
 * Component class for Infinispan caches
 * @author Heiko W. Rupp
 */
public class IspnCacheComponent extends MetricsRemappingComponent<IspnCacheComponent> implements CreateChildResourceFacet {
    static final String FLAVOR = "_flavor";

    /**
     * Create embedded loader/store elements
     * Ttick is to take the _flavor property and turn it into a part of the path
     * @param  report contains all of the necessary information to create the specified resource and should be populated
     *                with the
     *
     * @return
     */
    @Override
    public CreateResourceReport createResource(CreateResourceReport report) {

        Configuration config = report.getResourceConfiguration();
        String flavor = config.getSimpleValue(FLAVOR, null);
        if (flavor==null) {
            report.setStatus(CreateResourceStatus.INVALID_CONFIGURATION);
            report.setErrorMessage("No flavor given");

            return report;
        }
        String newName = report.getUserSpecifiedResourceName();
        Address address = new Address(this.getAddress());
        address.add(flavor,newName);
        Operation add = new Operation("add",address);
        for (Property prop: config.getProperties()) {
            if (prop.getName().equals(FLAVOR)) {
                continue;
            }
            if (prop instanceof PropertyList) {
                List<Property> list = ((PropertyList)prop).getList();
                Map<String, String> properties = new HashMap<String, String>();
                for (Property property : list) {
                    if (property instanceof PropertyMap) {
                        PropertyMap propertyMap = (PropertyMap)property;
                        properties.put(((PropertySimple)propertyMap.get("name")).getStringValue(),
                                ((PropertySimple)propertyMap.get("value")).getStringValue());
                    }
                }
                List<Object> propList = new ArrayList<Object>();
                propList.add(properties);
                add.addAdditionalProperty(prop.getName(), propList);
            }
            else if (prop instanceof PropertySimple) {
                PropertySimple ps = (PropertySimple) prop;
                add.addAdditionalProperty(prop.getName(), ps.getStringValue()); // TODO format conversion?
            }
            else {
                throw new IllegalArgumentException("Unsupported Property provided, was " + prop.getClass().getName());
            }

        }
        Result result = getASConnection().execute(add);
        if (result.isSuccess()) {
            report.setResourceKey(address.getPath());
            report.setResourceName(address.getPath());
            report.setStatus(CreateResourceStatus.SUCCESS);
        }
        else {
            report.setErrorMessage(result.getFailureDescription());
            report.setStatus(CreateResourceStatus.FAILURE);
        }

        return report;
    }

    public void getValues(MeasurementReport report, Set<MeasurementScheduleRequest> metrics) throws Exception {
        Set<MeasurementScheduleRequest> requests = metrics;
        Set<MeasurementScheduleRequest> todo = new HashSet<MeasurementScheduleRequest>();
        for (MeasurementScheduleRequest req : requests) {
            if (req.getName().equals("__flavor")) {
                String flavor = getCacheFlavorFromPath();
                MeasurementDataTrait trait = new MeasurementDataTrait(req, flavor);
                report.addData(trait);
            } else {
                todo.add(req);
            }
        }
        super.getValues(report, todo);
    }

    private String getCacheFlavorFromPath() {
        String flavor = getPath().substring(getPath().lastIndexOf(",") + 1);
        flavor = flavor.substring(0, flavor.indexOf("="));
        return flavor;
    }

    @Override
    public Configuration loadResourceConfiguration() throws Exception {
        Configuration config = super.loadResourceConfiguration();
        String f = getCacheFlavorFromPath();
        PropertySimple flavor = new PropertySimple(FLAVOR, f);
        config.put(flavor);
        return config;
    }

    @Override
    public void updateResourceConfiguration(ConfigurationUpdateReport report) {

        report.getConfiguration().remove(FLAVOR);

        super.updateResourceConfiguration(report);
    }
}
