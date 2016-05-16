/*
 * Copyright 2012-2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.boot.autoconfigure.cassandra;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.LatencyAwarePolicy;
import com.datastax.driver.core.policies.LatencyAwarePolicy.Builder;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.google.common.collect.Lists;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for Cassandra.
 *
 * @author Julien Dubois
 * @author Phillip Webb
 * @author Alexander Dejanovski 
 * @since 1.3.0
 */
@ConfigurationProperties(prefix = "spring.data.cassandra")
public class CassandraProperties {

	/**
	 * Keyspace name to use.
	 */
	private String keyspaceName;

	/**
	 * Name of the Cassandra cluster.
	 */
	private String clusterName;

	/**
	 * Comma-separated list of cluster node addresses.
	 */
	private String contactPoints = "localhost";

	/**
	 * Port of the Cassandra server.
	 */
	private int port = ProtocolOptions.DEFAULT_PORT;

	/**
	 * Compression supported by the Cassandra binary protocol.
	 */
	private Compression compression = Compression.NONE;

	/**
	 * Class name of the load balancing policy.
	 */
	private String loadBalancingPolicy;

	/**
	 * Queries consistency level.
	 */
	private ConsistencyLevel consistencyLevel;

	/**
	 * Queries serial consistency level.
	 */
	private ConsistencyLevel serialConsistencyLevel;

	/**
	 * Queries default fetch size.
	 */
	private int fetchSize = QueryOptions.DEFAULT_FETCH_SIZE;

	/**
	 * Reconnection policy class.
	 */
	private String reconnectionPolicy;

	/**
	 * Class name of the retry policy.
	 */
	private String retryPolicy;

	/**
	 * Socket option: connection time out.
	 */
	private int connectTimeoutMillis = SocketOptions.DEFAULT_CONNECT_TIMEOUT_MILLIS;

	/**
	 * Socket option: read time out.
	 */
	private int readTimeoutMillis = SocketOptions.DEFAULT_READ_TIMEOUT_MILLIS;

	/**
	 * Enable SSL support.
	 */
	private boolean ssl = false;

	private String user = "";

	/**
	 * password for authentication.
	 */
	private String password = "";

	public String getKeyspaceName() {
		return this.keyspaceName;
	}

	public void setKeyspaceName(String keyspaceName) {
		this.keyspaceName = keyspaceName;
	}

	public String getClusterName() {
		return this.clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	public String getContactPoints() {
		return this.contactPoints;
	}

	public void setContactPoints(String contactPoints) {
		this.contactPoints = contactPoints;
	}

	public int getPort() {
		return this.port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public Compression getCompression() {
		return this.compression;
	}

	public void setCompression(Compression compression) {
		this.compression = compression;
	}

	public LoadBalancingPolicy getLoadBalancingPolicy() throws InstantiationException, IllegalAccessException, ClassNotFoundException, NoSuchMethodException, SecurityException, IllegalArgumentException, InvocationTargetException {
		return parseLbPolicy(this.loadBalancingPolicy);
	}

	public void setLoadBalancingPolicy(
			String loadBalancingPolicy) {
		this.loadBalancingPolicy = loadBalancingPolicy;
	}

	public ConsistencyLevel getConsistencyLevel() {
		return this.consistencyLevel;
	}

	public void setConsistencyLevel(ConsistencyLevel consistency) {
		this.consistencyLevel = consistency;
	}

	public ConsistencyLevel getSerialConsistencyLevel() {
		return this.serialConsistencyLevel;
	}

	public void setSerialConsistencyLevel(ConsistencyLevel serialConsistency) {
		this.serialConsistencyLevel = serialConsistency;
	}

	public int getFetchSize() {
		return this.fetchSize;
	}

	public void setFetchSize(int fetchSize) {
		this.fetchSize = fetchSize;
	}

	public ReconnectionPolicy getReconnectionPolicy() throws InstantiationException, IllegalAccessException, ClassNotFoundException, NoSuchMethodException, SecurityException, IllegalArgumentException, InvocationTargetException {
		return parseReconnectionPolicy(this.reconnectionPolicy);
	}

	public void setReconnectionPolicy(
			String reconnectionPolicy) {
		this.reconnectionPolicy = reconnectionPolicy;
	}

	public RetryPolicy getRetryPolicy() throws InstantiationException, IllegalAccessException, ClassNotFoundException, NoSuchMethodException, SecurityException, IllegalArgumentException, InvocationTargetException, NoSuchFieldException {
		return parseRetryPolicy(this.retryPolicy);
	}

	public void setRetryPolicy(String retryPolicy) {
		this.retryPolicy = retryPolicy;
	}

	public int getConnectTimeoutMillis() {
		return this.connectTimeoutMillis;
	}

	public void setConnectTimeoutMillis(int connectTimeoutMillis) {
		this.connectTimeoutMillis = connectTimeoutMillis;
	}

	public int getReadTimeoutMillis() {
		return this.readTimeoutMillis;
	}

	public void setReadTimeoutMillis(int readTimeoutMillis) {
		this.readTimeoutMillis = readTimeoutMillis;
	}

	public boolean isSsl() {
		return this.ssl;
	}

	public void setSsl(boolean ssl) {
		this.ssl = ssl;
	}

	public String getUser() {
		return this.user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPassword() {
		return this.password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public LoadBalancingPolicy parseLbPolicy(String loadBalancingPolicyString) throws InstantiationException,
			IllegalAccessException, ClassNotFoundException,
			NoSuchMethodException, SecurityException, IllegalArgumentException,
			InvocationTargetException {
		String lb_regex = "([a-zA-Z\\.]*Policy)(\\()(.*)(\\))";
		Pattern lb_pattern = Pattern.compile(lb_regex);
		if (!loadBalancingPolicyString.contains("(")) {
			loadBalancingPolicyString += "()";
		}
		Matcher lb_matcher = lb_pattern.matcher(loadBalancingPolicyString);

		if (lb_matcher.matches()) {
			if (lb_matcher.groupCount() > 0) {
				// Primary LB policy has been specified
				String primaryLoadBalancingPolicy = lb_matcher.group(1);
				String loadBalancingPolicyParams = lb_matcher.group(3);
				return getLbPolicy(primaryLoadBalancingPolicy, loadBalancingPolicyParams);
			}
		}

		return null;
	}

	@SuppressWarnings("rawtypes")
	public LoadBalancingPolicy getLbPolicy(String lbString, String parameters) throws ClassNotFoundException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		LoadBalancingPolicy policy = null;
		if (!lbString.contains(".")) {
			lbString = "com.datastax.driver.core.policies." + lbString;
		}

		if (parameters.length() > 0) {
			// Child policy or parameters have been specified
			String paramsRegex = "([^,]+\\(.+?\\))|([^,]+)";
			Pattern param_pattern = Pattern.compile(paramsRegex);
			Matcher lb_matcher = param_pattern.matcher(parameters);

			ArrayList<Object> paramList = Lists.newArrayList();
			ArrayList<Class> primaryParametersClasses = Lists.newArrayList();
			int nbParameters = 0;
			while (lb_matcher.find() && lb_matcher.groupCount() > 0) {
				if (lb_matcher.group().contains("(")
						&& !lb_matcher.group().trim().startsWith("(")) {
					// We are dealing with child policies here
					primaryParametersClasses.add(LoadBalancingPolicy.class);
					// Parse and add child policy to the parameter list
					paramList.add(parseLbPolicy(lb_matcher.group()));
					nbParameters++;
				}
				else {
					// We are dealing with parameters that are not policies
					// here
					String param = lb_matcher.group();
					if (param.contains("'") || param.contains("\"")) {
						primaryParametersClasses.add(String.class);
						paramList.add(new String(param.trim().replace("'", "").replace("\"", "")));
					}
					else if (param.contains(".")
							|| param.toLowerCase().contains("(double)")
							|| param.toLowerCase().contains("(float)")) {
						// gotta allow using float or double
						if (param.toLowerCase().contains("(double)")) {
							primaryParametersClasses.add(double.class);
							paramList.add(Double.parseDouble(param.replace("(double)", "").trim()));
						}
						else {
							primaryParametersClasses.add(float.class);
							paramList.add(Float.parseFloat(param.replace("(float)", "").trim()));
						}
					}
					else {
						if (param.toLowerCase().contains("(long)")) {
							primaryParametersClasses.add(long.class);
							paramList.add(Long.parseLong(param.toLowerCase().replace("(long)", "").trim()));
						}
						else {
							primaryParametersClasses.add(int.class);
							paramList.add(Integer.parseInt(param.toLowerCase().replace("(int)", "").trim()));
						}
					}
					nbParameters++;
				}
			}

			if (nbParameters > 0) {
				// Instantiate load balancing policy with parameters
				if (lbString.toLowerCase().contains("latencyawarepolicy")) {
					// special sauce for the latency aware policy which uses a
					// builder subclass to instantiate
					Builder builder = LatencyAwarePolicy.builder((LoadBalancingPolicy) paramList.get(0));

					builder.withExclusionThreshold((Double) paramList.get(1));
					builder.withScale((Long) paramList.get(2), TimeUnit.MILLISECONDS);
					builder.withRetryPeriod((Long) paramList.get(3), TimeUnit.MILLISECONDS);
					builder.withUpdateRate((Long) paramList.get(4), TimeUnit.MILLISECONDS);
					builder.withMininumMeasurements((Integer) paramList.get(5));

					return builder.build();

				}
				else {
					Class<?> clazz = Class.forName(lbString);
					Constructor<?> constructor = clazz.getConstructor(primaryParametersClasses.toArray(new Class[primaryParametersClasses.size()]));

					return (LoadBalancingPolicy) constructor.newInstance(paramList.toArray(new Object[paramList.size()]));
				}
			}
			else {
				// Only one policy has been specified, with no parameter or
				// child policy
				Class<?> clazz = Class.forName(lbString);
				policy = (LoadBalancingPolicy) clazz.newInstance();

				return policy;
			}

		}
		else {
			// Only one policy has been specified, with no parameter or child
			// policy

			Class<?> clazz = Class.forName(lbString);
			policy = (LoadBalancingPolicy) clazz.newInstance();

			return policy;
		}
	}

	public RetryPolicy parseRetryPolicy(String retryPolicyString) throws InstantiationException, IllegalAccessException, ClassNotFoundException, NoSuchMethodException, SecurityException, IllegalArgumentException, InvocationTargetException, NoSuchFieldException {
		if (!retryPolicyString.contains(".")) {
			retryPolicyString = "com.datastax.driver.core.policies." + retryPolicyString;
			Class<?> clazz = Class.forName(retryPolicyString);
			Field field = clazz.getDeclaredField("INSTANCE");
			RetryPolicy policy = (RetryPolicy) field.get(null);

			return policy;
		}

		return null;
	}

	public ReconnectionPolicy parseReconnectionPolicy(String reconnectionPolicyString) throws InstantiationException, IllegalAccessException, ClassNotFoundException, NoSuchMethodException, SecurityException, IllegalArgumentException, InvocationTargetException {
		String lb_regex = "([a-zA-Z\\.]*Policy)(\\()(.*)(\\))";
		Pattern lb_pattern = Pattern.compile(lb_regex);
		Matcher lb_matcher = lb_pattern.matcher(reconnectionPolicyString);

		if (lb_matcher.matches()) {
			if (lb_matcher.groupCount() > 0) {
				// Primary LB policy has been specified
				String primaryReconnectionPolicy = lb_matcher.group(1);
				String reconnectionPolicyParams = lb_matcher.group(3);
				return getReconnectionPolicy(primaryReconnectionPolicy, reconnectionPolicyParams);
			}
		}

		return null;
	}

	@SuppressWarnings("rawtypes")
	public ReconnectionPolicy getReconnectionPolicy(String rcString, String parameters) throws ClassNotFoundException,
			NoSuchMethodException, SecurityException, InstantiationException,
			IllegalAccessException, IllegalArgumentException,
			InvocationTargetException {
		ReconnectionPolicy policy = null;
		// ReconnectionPolicy childPolicy = null;
		if (!rcString.contains(".")) {
			rcString = "com.datastax.driver.core.policies." + rcString;
		}

		if (parameters.length() > 0) {
			// Child policy or parameters have been specified
			String paramsRegex = "([^,]+\\(.+?\\))|([^,]+)";
			Pattern param_pattern = Pattern.compile(paramsRegex);
			Matcher lb_matcher = param_pattern.matcher(parameters);

			ArrayList<Object> paramList = Lists.newArrayList();
			ArrayList<Class> primaryParametersClasses = Lists.newArrayList();
			int nbParameters = 0;
			while (lb_matcher.find() && lb_matcher.groupCount() > 0) {
				if (lb_matcher.group().contains("(")
						&& !lb_matcher.group().trim().startsWith("(")) {
					// We are dealing with child policies here
					primaryParametersClasses.add(LoadBalancingPolicy.class);
					// Parse and add child policy to the parameter list
					paramList.add(parseReconnectionPolicy(lb_matcher.group()));
					nbParameters++;
				}
				else {
					// We are dealing with parameters that are not policies
					// here
					String param = lb_matcher.group();
					if (param.contains("'") || param.contains("\"")) {
						primaryParametersClasses.add(String.class);
						paramList.add(new String(param.trim().replace("'", "").replace("\"", "")));
					}
					else if (param.contains(".")
							|| param.toLowerCase().contains("(double)")
							|| param.toLowerCase().contains("(float)")) {
						// gotta allow using float or double
						if (param.toLowerCase().contains("(double)")) {
							primaryParametersClasses.add(double.class);
							paramList.add(Double.parseDouble(param.replace("(double)", "").trim()));
						}
						else {
							primaryParametersClasses.add(float.class);
							paramList.add(Float.parseFloat(param.replace("(float)", "").trim()));
						}
					}
					else {
						if (param.toLowerCase().contains("(long)")) {
							primaryParametersClasses.add(long.class);
							paramList.add(Long.parseLong(param.toLowerCase().replace("(long)", "").trim()));
						}
						else {
							primaryParametersClasses.add(int.class);
							paramList.add(Integer.parseInt(param.toLowerCase().replace("(int)", "").trim()));
						}
					}
					nbParameters++;
				}
			}

			if (nbParameters > 0) {
				// Instantiate load balancing policy with parameters
				Class<?> clazz = Class.forName(rcString);
				Constructor<?> constructor = clazz.getConstructor(primaryParametersClasses.toArray(new Class[primaryParametersClasses.size()]));

				return (ReconnectionPolicy) constructor.newInstance(paramList.toArray(new Object[paramList.size()]));

			}
			// Only one policy has been specified, with no parameter or child
			// policy
			Class<?> clazz = Class.forName(rcString);
			policy = (ReconnectionPolicy) clazz.newInstance();

			return policy;

		}
		Class<?> clazz = Class.forName(rcString);
		policy = (ReconnectionPolicy) clazz.newInstance();

		return policy;
	}

}
