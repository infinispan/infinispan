package org.infinispan.server.endpoint.deployments;

import org.infinispan.server.endpoint.Constants;
import org.infinispan.server.endpoint.subsystem.ExtensionManagerService;
import org.jboss.as.server.deployment.Attachments;
import org.jboss.as.server.deployment.DeploymentPhaseContext;
import org.jboss.as.server.deployment.annotation.CompositeIndex;
import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.AnnotationTarget;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.jboss.msc.service.Service;
import org.jboss.msc.service.ServiceBuilder;
import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.ServiceName;
import org.jboss.msc.value.InjectedValue;

import java.util.List;

import static org.infinispan.server.endpoint.EndpointLogger.ROOT_LOGGER;

public abstract class AbstractNamedFactoryExtensionProcessor<T> extends AbstractServerExtensionProcessor<T> {

    private static final DotName NAMED_FACTORY = DotName.createSimple("org.infinispan.notifications.cachelistener.filter.NamedFactory");

    private final ServiceName extensionManagerServiceName;

    protected AbstractNamedFactoryExtensionProcessor(ServiceName extensionManagerServiceName) {
        this.extensionManagerServiceName = extensionManagerServiceName;
    }

    @Override
    public final void installService(DeploymentPhaseContext ctx, String serviceName, T instance) {
        CompositeIndex index = ctx.getDeploymentUnit().getAttachment(Attachments.COMPOSITE_ANNOTATION_INDEX);
        List<AnnotationInstance> annotations = index.getAnnotations(NAMED_FACTORY);
        if (annotations.isEmpty())
            ROOT_LOGGER.noFactoryName(getServiceClass().getName());
        else {
            for (AnnotationInstance annotation : annotations) {
                AnnotationTarget annotationTarget = annotation.target();
                if (annotationTarget instanceof ClassInfo) {
                    ClassInfo classInfo = (ClassInfo) annotationTarget;
                    DotName target = DotName.createSimple(serviceName);
                    if (classInfo.name().equals(target)) {
                        String nameValue = annotation.value("name").asString();
                        AbstractExtensionManagerService<T> service = createService(nameValue, instance);
                        ServiceName extensionServiceName = Constants.DATAGRID.append(service.getServiceTypeName(), nameValue.replaceAll("\\.", "_"));
                        ServiceBuilder<T> serviceBuilder = ctx.getServiceTarget().addService(extensionServiceName, service);
                        serviceBuilder.setInitialMode(ServiceController.Mode.ACTIVE)
                                .addDependency(extensionManagerServiceName, ExtensionManagerService.class, service.getExtensionManager());
                        serviceBuilder.install();
                    }
                }
            }
        }
    }

    public abstract AbstractExtensionManagerService<T> createService(String name, T instance);

    public static abstract class AbstractExtensionManagerService<T> implements Service<T> {
        protected final String name;
        protected final T extension;
        protected final InjectedValue<ExtensionManagerService> extensionManager = new InjectedValue<>();

        protected AbstractExtensionManagerService(String name, T extension) {
            assert name != null : ROOT_LOGGER.nullVar("name");
            assert extension != null : ROOT_LOGGER.nullVar(getServiceTypeName());
            this.extension = extension;
            this.name = name;
        }

        public InjectedValue<ExtensionManagerService> getExtensionManager() {
            return extensionManager;
        }

        public abstract String getServiceTypeName();
    }

}
