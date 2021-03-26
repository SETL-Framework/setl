package io.github.setl.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The Experimental annotation indicate that the annotated class/method/field is supposed to be an experimental feature,
 * thus the stability can't be guaranteed.
 */
@Retention(RetentionPolicy.CLASS)
@Target({ElementType.FIELD, ElementType.METHOD, ElementType.TYPE})
public @interface Experimental {
}
