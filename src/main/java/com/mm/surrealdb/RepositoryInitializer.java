package com.mm.surrealdb;

import com.mm.surrealdb.annotation.SurrealQuery;
import com.surrealdb.driver.model.QueryResult;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.*;
import org.springframework.context.annotation.Configuration;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.mm.surrealdb.SurrealDBConnection.getRepoDriver;

@Configuration
public class RepositoryInitializer implements BeanDefinitionRegistryPostProcessor {
    public Set<Class<? extends SurrealCrudRepository>> findSurrealCrudRepositories(String packageName) {
        Reflections reflections = new Reflections(new ConfigurationBuilder()
            .setUrls(ClasspathHelper.forPackage(packageName))
            .setScanners(new SubTypesScanner(false)));

        return reflections.getSubTypesOf(SurrealCrudRepository.class);
    }

    @Override
    public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) throws BeansException {
        Set<Class<? extends SurrealCrudRepository>> allClasses = findSurrealCrudRepositories("");
        for (Class<? extends SurrealCrudRepository> repoClass : allClasses) {
            Type[] genericInterfaces = repoClass.getGenericInterfaces();
            for (Type genericInterface : genericInterfaces) {
                if (genericInterface instanceof ParameterizedType) {
                    ParameterizedType parameterizedType = (ParameterizedType) genericInterface;
                    if (parameterizedType.getRawType().equals(SurrealCrudRepository.class)) {
                        Type[] typeArguments = parameterizedType.getActualTypeArguments();
                        if (typeArguments.length == 2) {
                            Type entityType = typeArguments[0];
                            Class<?> entityClass = null;
                            if (entityType instanceof Class<?>) {
                                entityClass = (Class<?>) entityType;
                            }
                            registerRepositoryBean(entityClass, registry, repoClass);
                        }
                    }
                }
            }
        }
    }

    private <T, ID, R extends SurrealCrudRepository<T, ID>> void registerRepositoryBean(Class<T> entityType, BeanDefinitionRegistry registry, Class<R> repositoryClass) {
        ProxyFactory proxyFactory = createProxyFactory(repositoryClass, entityType);
        AbstractBeanDefinition beanDefinition = createBeanDefinition(repositoryClass, proxyFactory);
        registry.registerBeanDefinition(repositoryClass.getSimpleName(), beanDefinition);
    }

    private <T, ID, R extends SurrealCrudRepository<T, ID>> ProxyFactory createProxyFactory(Class<R> repositoryClass, Class<T> entityType) {
        SurrealCrudRepositoryImpl<T, ID> target = new SurrealCrudRepositoryImpl<>(entityType);
        ProxyFactory proxyFactory = new ProxyFactory();
        proxyFactory.addInterface(repositoryClass);
        proxyFactory.addAdvice((MethodInterceptor) invocation -> handleMethodInvocation(invocation, target));
        return proxyFactory;
    }

    private <T, ID> Object handleMethodInvocation(MethodInvocation invocation, SurrealCrudRepositoryImpl<T, ID> target) throws Throwable {
        Method method = invocation.getMethod();
        if (method.isAnnotationPresent(SurrealQuery.class)) {
            return handleSurrealQuery(invocation, method);
        } else {
            return method.invoke(target, invocation.getArguments());
        }
    }

    private Object handleSurrealQuery(MethodInvocation invocation, Method method) throws SQLException {
        String query = method.getAnnotation(SurrealQuery.class).value();
        Object[] args = invocation.getArguments();
        Pattern pattern = Pattern.compile("\\?(\\d+)");
        Matcher matcher = pattern.matcher(query);
        StringBuilder sb = new StringBuilder();
        int argIndex = 0;
        while (matcher.find()) {
            int index = Integer.parseInt(matcher.group(1)) - 1;
            if (index >= args.length) {
                throw new SQLException(String.format("Argument index out of bounds: %d", (index + 1)));
            }
            String replacement = args[index] instanceof String ? "'" + escapeReplacement(args[index].toString()) + "'" : args[index].toString();
            matcher.appendReplacement(sb, replacement);
            argIndex++;
        }
        matcher.appendTail(sb);
        if (argIndex != args.length) {
            throw new SQLException("Number of placeholders in the query doesn't match the number of arguments.");
        }
        String processedQuery = sb.toString();
        Type returnType = method.getGenericReturnType();
        return processReturnType(returnType, processedQuery, method);
    }

    private Object processReturnType(Type returnType, String processedQuery, Method method) throws SQLException {
        if (returnType instanceof ParameterizedType) {
            Type[] typeArguments = ((ParameterizedType) returnType).getActualTypeArguments();
            if (typeArguments.length > 0 && typeArguments[0] instanceof Class<?> genericClass) {
                System.out.println(genericClass);
                return processGenericReturnType((ParameterizedType) returnType, processedQuery, genericClass);
            }
        } else {
            return processNonParameterizedReturnType(processedQuery, returnType);
        }
        throw new SQLException(String.format("Unsupported return type for method: %s", method.getName()));
    }

    private Object processGenericReturnType(ParameterizedType returnType, String processedQuery, Type genericClassType) {
        final Class<?> rawType = (Class<?>) returnType.getRawType();
        Class<?> genericClass = (Class<?>) genericClassType;
        if (rawType.equals(List.class)) {
            return getRepoDriver().query(processedQuery, Collections.emptyMap(), genericClass)
                    .stream()
                    .flatMap(qr -> qr.getResult().stream())
                    .toList();
        } else if (rawType.equals(Optional.class)) {
            return getRepoDriver().query(processedQuery, Collections.emptyMap(), genericClass)
                    .stream()
                    .flatMap(qr -> qr.getResult().stream())
                    .findFirst();
        } else {
            return getRepoDriver().query(processedQuery, Collections.emptyMap(), genericClass);
        }
    }

    private <T> Object processNonParameterizedReturnType(String processedQuery, Type returnType) {
        List<QueryResult<T>> results = getRepoDriver().query(processedQuery, Collections.emptyMap(), (Class<? extends T>) returnType);
        var result = results.stream()
                .flatMap(qr -> qr.getResult().stream())
                .toList();
        if (result.isEmpty()) {
            return null;
        }
        return result.get(0);
    }

    private <T, ID, R extends SurrealCrudRepository<T, ID>> AbstractBeanDefinition createBeanDefinition(Class<R> repositoryClass, ProxyFactory proxyFactory) {
        return BeanDefinitionBuilder
                .genericBeanDefinition(repositoryClass, () -> repositoryClass.cast(proxyFactory.getProxy()))
                .getBeanDefinition();
    }

    private static String escapeReplacement(String replacement) {
        return Matcher.quoteReplacement(replacement);
    }
    @Override
    public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {
    }

}
