package org.infinispan.server.rhq;

import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.configuration.ConfigurationUpdateStatus;
import org.rhq.core.domain.configuration.PropertySimple;
import org.rhq.core.domain.configuration.definition.ConfigurationDefinition;
import org.rhq.core.domain.measurement.MeasurementDataTrait;
import org.rhq.core.domain.measurement.MeasurementReport;
import org.rhq.core.domain.measurement.MeasurementScheduleRequest;
import org.rhq.core.domain.resource.CreateResourceStatus;
import org.rhq.core.pluginapi.configuration.ConfigurationUpdateReport;
import org.rhq.core.pluginapi.inventory.CreateChildResourceFacet;
import org.rhq.core.pluginapi.inventory.CreateResourceReport;
import org.rhq.modules.plugins.jbossas7.ConfigurationWriteDelegate;
import org.rhq.modules.plugins.jbossas7.json.Address;

import java.util.HashSet;
import java.util.Set;

/**
 * Component class for Infinispan caches
 * @author Heiko W. Rupp
 */
public class IspnCacheComponent extends MetricsRemappingComponent<IspnCacheComponent> implements CreateChildResourceFacet {
    static final String FLAVOR = "_flavor";

    @Override
    public CreateResourceReport createResource(CreateResourceReport report) {
         report = super.createResource(report);

        // Since our properties are can be added at parent resource creation time, we have to make sure they are added.
        if (report.getStatus() == CreateResourceStatus.SUCCESS) {
            // Now we have to send this as an update, so the properties are created properly
            ConfigurationUpdateReport updateReport = new ConfigurationUpdateReport(report.getResourceConfiguration());
            ConfigurationDefinition configDef = report.getResourceType().getResourceConfigurationDefinition();
            Address address = new Address(getAddress());
            address.add(report.getPluginConfiguration().getSimpleValue("path"),report.getUserSpecifiedResourceName());
            ConfigurationWriteDelegate delegate = new ConfigurationWriteDelegate(configDef, getASConnection(), address);
            delegate.updateResourceConfiguration(updateReport);

            if (updateReport.getStatus() != ConfigurationUpdateStatus.SUCCESS) {
                report.setErrorMessage(updateReport.getErrorMessage());
                report.setStatus(CreateResourceStatus.FAILURE);
            }
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
