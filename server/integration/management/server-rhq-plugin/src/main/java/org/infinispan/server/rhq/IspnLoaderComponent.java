package org.infinispan.server.rhq;

import org.rhq.core.domain.configuration.*;
import org.rhq.core.domain.measurement.MeasurementDataTrait;
import org.rhq.core.domain.measurement.MeasurementReport;
import org.rhq.core.domain.measurement.MeasurementScheduleRequest;
import org.rhq.core.pluginapi.configuration.ConfigurationUpdateReport;
import org.rhq.core.pluginapi.inventory.CreateChildResourceFacet;
import org.rhq.modules.plugins.jbossas7.BaseComponent;

import java.util.*;

/**
 * Component class for Infinispan loaders/stores
 * @author William Burns
 */
public class IspnLoaderComponent extends BaseComponent<IspnLoaderComponent> implements CreateChildResourceFacet {
    static final String FLAVOR = "_flavor";

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
