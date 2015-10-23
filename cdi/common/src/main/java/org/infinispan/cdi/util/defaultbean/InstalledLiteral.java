package org.infinispan.cdi.util.defaultbean;

import javax.enterprise.util.AnnotationLiteral;

public class InstalledLiteral extends AnnotationLiteral<Installed> implements Installed {

    public static final Installed INSTANCE = new InstalledLiteral(); 
    
}
