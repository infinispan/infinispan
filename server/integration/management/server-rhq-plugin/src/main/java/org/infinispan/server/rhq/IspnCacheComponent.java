package org.infinispan.server.rhq;

import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.configuration.ConfigurationUpdateStatus;
import org.rhq.core.domain.configuration.PropertyMap;
import org.rhq.core.domain.configuration.PropertySimple;
import org.rhq.core.domain.configuration.definition.ConfigurationDefinition;
import org.rhq.core.domain.configuration.definition.PropertyDefinition;
import org.rhq.core.domain.configuration.definition.PropertyDefinitionMap;
import org.rhq.core.domain.configuration.definition.PropertyDefinitionSimple;
import org.rhq.core.domain.measurement.AvailabilityType;
import org.rhq.core.domain.measurement.MeasurementDataTrait;
import org.rhq.core.domain.measurement.MeasurementReport;
import org.rhq.core.domain.measurement.MeasurementScheduleRequest;
import org.rhq.core.domain.resource.CreateResourceStatus;
import org.rhq.core.pluginapi.configuration.ConfigurationUpdateReport;
import org.rhq.core.pluginapi.inventory.CreateChildResourceFacet;
import org.rhq.core.pluginapi.inventory.CreateResourceReport;
import org.rhq.modules.plugins.jbossas7.ASConnection;
import org.rhq.modules.plugins.jbossas7.ConfigurationWriteDelegate;
import org.rhq.modules.plugins.jbossas7.CreateResourceDelegate;
import org.rhq.modules.plugins.jbossas7.json.Address;
import org.rhq.modules.plugins.jbossas7.json.ReadAttribute;
import org.rhq.modules.plugins.jbossas7.json.Result;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Component class for Infinispan caches
 * @author Heiko W. Rupp
 * @author William Burns
 */
public class IspnCacheComponent extends MetricsRemappingComponent<IspnCacheComponent> implements CreateChildResourceFacet {
    static final String FLAVOR = "_flavor";

   /**
    * Get the availability check based on if the cache is actually running or not
    * @see org.rhq.core.pluginapi.inventory.ResourceComponent#getAvailability()
    */
   @Override
   public AvailabilityType getAvailability() {
      ReadAttribute op = new ReadAttribute(getAddress(), "cache-status");
      Result res = getASConnection().execute(op);
      if (res != null && res.isSuccess()) {
         if ("RUNNING".equals(res.getResult())) {
             return AvailabilityType.UP;
         }
      }
      return AvailabilityType.DOWN;
   }

   @Override
    public CreateResourceReport createResource(CreateResourceReport report) {
        if (report.getPluginConfiguration().getSimpleValue("path").contains("jdbc")) {
            ASConnection connection = getASConnection();
            ConfigurationDefinition configDef = report.getResourceType().getResourceConfigurationDefinition();

            CreateResourceDelegate delegate = new CreateResourceDelegate(configDef, connection, getAddress()) {
               @Override
               protected Map<String, Object> prepareSimplePropertyMap(PropertyMap property, PropertyDefinitionMap propertyDefinition) {
                  // Note this is all pretty much copied from ConfigurationWriteDelegate.prepareSimplePropertyMap
                  Map<String,PropertyDefinition> memberDefinitions = propertyDefinition.getMap();

                  Map<String,Object> results = new HashMap<String,Object>();
                  for (String name : memberDefinitions.keySet()) {
                     PropertyDefinition memberDefinition = memberDefinitions.get(name);

                     if (memberDefinition instanceof PropertyDefinitionSimple) {
                        PropertyDefinitionSimple pds = (PropertyDefinitionSimple) memberDefinition;
                        PropertySimple ps = (PropertySimple) property.get(name);
                        if ((ps==null || ps.getStringValue()==null ) && !pds.isRequired())
                           continue;
                        if (ps!=null)
                           results.put(name,ps.getStringValue());
                     }
                     // This is added since it isn't supported already.
                     // Should be merged with https://github.com/rhq-project/rhq/pull/128
                     else if (memberDefinition instanceof PropertyDefinitionMap) {
                        PropertyDefinitionMap pdm = (PropertyDefinitionMap) memberDefinition;
                        PropertyMap pm = (PropertyMap) property.get(name);
                        if ((pm==null || pm.getMap().isEmpty()) && !pdm.isRequired())
                           continue;
                        if (pm != null) {
                           Map<String, Object> innerMap = prepareSimplePropertyMap(pm, pdm);
                           results.put(name, innerMap);
                        }
                     }
                     else {
                        log.error(" *** not yet supported *** : " + memberDefinition.getName());
                     }
                  }
                  return results;
               }
            };
            report = delegate.createResource(report);
        } else {

            report = super.createResource(report);
        }

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
