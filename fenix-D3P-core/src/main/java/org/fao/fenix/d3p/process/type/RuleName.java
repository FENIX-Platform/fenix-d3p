package org.fao.fenix.d3p.process.type;

import java.lang.annotation.*;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface RuleName {
    String value() default "";
}
