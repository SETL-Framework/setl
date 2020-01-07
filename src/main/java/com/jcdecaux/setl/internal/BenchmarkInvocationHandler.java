package com.jcdecaux.setl.internal;

import com.jcdecaux.setl.annotation.Benchmark;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class BenchmarkInvocationHandler implements InvocationHandler {

    private Object target;

    private final Map<String, Method> methods = new HashMap<>();

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

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

        Method targetMethod = methods.get(method.getName());
        Object result;

        if (targetMethod.isAnnotationPresent(Benchmark.class)) {
            long start = System.nanoTime();
            result = targetMethod.invoke(target, args);
            long elapsed = System.nanoTime() - start;

            logger.info("Executing " + target.getClass().getSimpleName() + "." +
                    method.getName() + " finished in " + elapsed + " ns");
        } else {
            result = targetMethod.invoke(target, args);
        }

        return result;
    }

}
