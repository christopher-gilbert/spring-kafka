/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.kafka.core;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanNotOfRequiredTypeException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Factory that allows beans to be looked up using the name of another bean.
 *
 * @param <T> the type of the mapped bean.
 * @author Chris Gilbert
 */
public class MappedBeanFactory<T> {

	private static final String DEFAULT_KEY = "default";

	private final BeanFactory beanFactory;

	private final Map<String, String> beanNameMappings = new ConcurrentHashMap<>();

	public MappedBeanFactory(BeanFactory beanFactory) {
		this.beanFactory = beanFactory;
	}

	/**
	 * Return a bean instance. If a bean has been added for the given key bean name then that will be returned.
	 * <p>
	 * Otherwise if a default bean has been registered then that will be returned.
	 * <p>
	 * If neither of the above is the case then null will be returned.
	 *
	 * @param keyBeanName the name of the bean to be used for the lookup.
	 * @param requiredType the expected type of the bean.
	 * @return the appropriate bean, or null.
	 * @throws BeanNotOfRequiredTypeException if the bean is not of the required type
	 */
	public T getOrDefault(String keyBeanName, Class<T> requiredType) {
		String mappedBeanName = this.beanNameMappings.getOrDefault(keyBeanName,
				this.beanNameMappings.get(DEFAULT_KEY));
		return this.beanFactory.getBean(mappedBeanName, requiredType);
	}


	/**
	 * Register the given bean name to be used as a default if a lookup is performed with a bean
	 * that has not been used as a key.
	 *
	 * @param mappedBeanName the name of the bean
	 * @throws NoSuchBeanDefinitionException if there is no bean with this name in the current application.
	 * @throws IllegalStateException         if there is already a default bean registered.
	 */
	public void addDefaultBeanMapping(String mappedBeanName) {
		addBeanMapping(DEFAULT_KEY, mappedBeanName);
	}

	/**
	 * Register the given bean name to be used if a lookup is performed for the given key.
	 *
	 * @param keyBeanName    the name of the bean used for a lookup.
	 * @param mappedBeanName the name of the bean
	 * @throws NoSuchBeanDefinitionException if either of the provided bean names do not match beans in the current
	 *                                       application.
	 * @throws IllegalStateException         if there is already a bean registered for the given key.
	 */
	public void addBeanMapping(String keyBeanName, String mappedBeanName) {
		validateBeans(keyBeanName, mappedBeanName);
		this.beanNameMappings.put(keyBeanName, mappedBeanName);
	}

	/**
	 * Return a set of all the beans that have been registered as default and against any keys.
	 *
	 * @return the set of bean names
	 */
	public Set<String> getAllMappedBeanNames() {
		return new HashSet<>(this.beanNameMappings.values());
	}

	/**
	 * Ensure that there is not already a bean for the same key, and also ensure that the BeanFactory contains the
	 * expected beans.
	 *
	 * @param keyBeanName    the name of the bean used for a lookup
	 * @param mappedBeanName the bean name
	 */
	private void validateBeans(String keyBeanName, String mappedBeanName) {
		if (this.beanNameMappings.containsKey(keyBeanName)) {
			throw new IllegalStateException(String.format("Attempt to map more than one bean to %s", keyBeanName));
		}
		if (this.beanFactory instanceof ConfigurableListableBeanFactory) {
			((ConfigurableListableBeanFactory) this.beanFactory).getBeanDefinition(mappedBeanName);

			if (!DEFAULT_KEY.equals(keyBeanName)) {
				((ConfigurableListableBeanFactory) this.beanFactory).getBeanDefinition(keyBeanName);
			}
		}
	}
}
