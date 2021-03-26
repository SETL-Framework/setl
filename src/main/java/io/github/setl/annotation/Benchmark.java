package io.github.setl.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * <P>The Benchmark annotation should be put on any class of Factory[T] to enable the benchmark process.
 * The total elapsed time of the factory will then be recorded. </p>
 *
 * <p>In addition, user can also put it onto any the "read", "process" or "write" methods that are defined
 * in AbstractFactory[T], and the elapsed time of each method will be recorded as well.</p>
 */
@InterfaceStability.Evolving
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.TYPE})
public @interface Benchmark {

}
