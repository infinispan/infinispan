package org.infinispan.server.endpoint.deployments;

import org.jboss.as.server.deployment.Attachments;
import org.jboss.as.server.deployment.DeploymentPhaseContext;
import org.jboss.as.server.deployment.annotation.CompositeIndex;
import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.AnnotationTarget;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.jboss.msc.service.ServiceBuilder;

import java.util.List;

import static org.infinispan.server.endpoint.EndpointLogger.ROOT_LOGGER;

public abstract class AbstractNamedFactoryExtensionProcessor<T> extends AbtractServerExtensionProcessor<T> {

    private static final DotName NAMED_FACTORY = DotName.createSimple("org.infinispan.filter.NamedFactory");

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
                        ServiceBuilder<T> serviceBuilder = buildService(ctx, nameValue, instance);
                        serviceBuilder.install();
                    }
                }
            }
        }
    }

    public abstract ServiceBuilder<T> buildService(DeploymentPhaseContext ctx, String name, T instance);

}
