package io.github.setl.internal;

import io.github.setl.annotation.Benchmark;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * BenchmarkInvocationHandler is used to handle the `@Benchmark` annotation. It measure the elapsed time of the method
 * having the annotation.
 */
public class BenchmarkInvocationHandler implements InvocationHandler {

    private Object target;

    private final Map<String, Method> methods = new HashMap<>();

    private Map<String, Double> benchmarkResult = new HashMap<>();

    private static Logger logger = LogManager.getLogger(BenchmarkInvocationHandler.class);

    public BenchmarkInvocationHandler(Object target) {
        this.target = target;
        for (Method method : target.getClass().getDeclaredMethods()) {
            // Exclude all the bridge methods
            if (!method.isBridge()) {
                this.methods.put(method.getName(), method);
            }
        }
    }

    public Map<String, Double> getBenchmarkResult() {
        return benchmarkResult;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

        Method targetMethod = methods.get(method.getName());
        Object result;

        if (targetMethod.isAnnotationPresent(Benchmark.class)) {
            // Measure the elapsed time if the method has @Benchmark annotation
            long start = System.nanoTime();
            result = targetMethod.invoke(target, args);
            long elapsed = System.nanoTime() - start;
            double seconds = (double)elapsed / 1_000_000_000.0;

            this.benchmarkResult.put(targetMethod.getName(), seconds);

            logger.info("Executing " + target.getClass().getSimpleName() + "." +
                    method.getName() + " finished in " + seconds + " s");
        } else {
            // if the method doesn't have the Benchmark annotation, run it without measuring the elapsed time
            result = targetMethod.invoke(target, args);
        }

        return result;
    }

}
