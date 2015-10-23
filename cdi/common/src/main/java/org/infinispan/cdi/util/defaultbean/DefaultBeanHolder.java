package org.infinispan.cdi.util.defaultbean;

import javax.enterprise.inject.spi.Bean;

public class DefaultBeanHolder {

    private final Bean<?> bean;
    
    public DefaultBeanHolder(Bean<?> bean) {
        this.bean = bean;
    }
    
    public Bean<?> getBean() {
        return bean;
    }
    
}
