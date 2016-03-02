package org.fao.fenix.d3p.flow;

import java.lang.annotation.*;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface FlowProperties {
    String name();
    boolean global();
    int priority();
}
