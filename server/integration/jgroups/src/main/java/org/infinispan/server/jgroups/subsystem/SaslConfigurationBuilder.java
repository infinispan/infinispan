package org.infinispan.server.jgroups.subsystem;

import org.infinispan.server.commons.service.InjectedValueDependency;
import org.infinispan.server.commons.service.ValueDependency;
import org.infinispan.server.jgroups.spi.SaslConfiguration;
import org.infinispan.server.jgroups.spi.TransportConfiguration;
import org.infinispan.server.jgroups.spi.service.ProtocolStackServiceName;
import org.jboss.as.domain.management.SecurityRealm;
import org.jboss.msc.service.ServiceBuilder;
import org.jboss.msc.service.ServiceName;
import org.jboss.msc.service.ServiceTarget;

public class SaslConfigurationBuilder extends AbstractProtocolConfigurationBuilder<SaslConfiguration>
        implements SaslConfiguration {

    private String mech;
    private String clusterRole;
    private ValueDependency<SecurityRealm> securityRealm;

    public SaslConfigurationBuilder(String stackName) {
        super(stackName, SaslConfiguration.PROTOCOL_NAME);
    }

    @Override
    public ServiceName getServiceName() {
        return ProtocolStackServiceName.CHANNEL_FACTORY.getServiceName(this.stackName).append("sasl");
    }

    @Override
    public ServiceBuilder<SaslConfiguration> build(ServiceTarget target) {
        ServiceBuilder<SaslConfiguration> builder = super.build(target);
        if (this.securityRealm != null) {
            this.securityRealm.register(builder);
        }
        return builder;
    }

    public SaslConfigurationBuilder setMech(String mech) {
        this.mech = mech;
        return this;
    }

    public SaslConfigurationBuilder setSecurityRealm(String securityRealm) {
        this.securityRealm = new InjectedValueDependency<>(SecurityRealm.ServiceUtil.createServiceName(securityRealm), SecurityRealm.class);
        return this;
    }

    public SaslConfigurationBuilder setClusterRole(String clusterRole) {
        this.clusterRole = clusterRole;
        return this;
    }

    @Override
    public SaslConfigurationBuilder addProperty(String name, String value) {
        super.addProperty(name, value);
        return this;
    }

    @Override
    public SaslConfiguration getValue() throws IllegalStateException, IllegalArgumentException {
        return this;
    }

    @Override
    public String getClusterRole() {
        return clusterRole;
    }

    @Override
    public SecurityRealm getSecurityRealm() {
        return (this.securityRealm != null) ? this.securityRealm.getValue() : null;
    }

    @Override
    public String getMech() {
        return mech;
    }
}
