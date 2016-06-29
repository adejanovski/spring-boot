/*
 * Copyright 2012-2016 the original author or authors.
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

import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import com.datastax.driver.core.policies.LatencyAwarePolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import org.junit.Test;
import static org.assertj.core.api.Assertions.assertThat;

public class CassandraPropertiesTest {

	@Test
	public void loadBalancingPolicyParsingTest() throws Exception {
		CassandraProperties properties = new CassandraProperties();
		String lbPolicyStr = "RoundRobinPolicy()";
		assertThat(properties.parseLbPolicy(lbPolicyStr) instanceof RoundRobinPolicy).isTrue();

		lbPolicyStr = "RoundRobinPolicy";
		assertThat(properties.parseLbPolicy(lbPolicyStr) instanceof RoundRobinPolicy).isTrue();

		lbPolicyStr = "TokenAwarePolicy(RoundRobinPolicy())";
		assertThat(properties.parseLbPolicy(lbPolicyStr) instanceof TokenAwarePolicy).isTrue();

		lbPolicyStr = "DCAwareRoundRobinPolicy(\"dc1\")";
		assertThat(properties.parseLbPolicy(lbPolicyStr) instanceof DCAwareRoundRobinPolicy).isTrue();

		lbPolicyStr = "TokenAwarePolicy(DCAwareRoundRobinPolicy(\"dc1\"))";
		assertThat(properties.parseLbPolicy(lbPolicyStr) instanceof TokenAwarePolicy).isTrue();

		lbPolicyStr = "LatencyAwarePolicy(TokenAwarePolicy(RoundRobinPolicy()),(double) 10.5,(long) 1,(long) 10,(long)1,10)";
		assertThat(properties.parseLbPolicy(lbPolicyStr) instanceof LatencyAwarePolicy).isTrue();
	}


	@Test
	public void retryPolicyParsingTest() throws Exception {
		CassandraProperties properties = new CassandraProperties();
		String retryPolicyStr = "DefaultRetryPolicy";
		assertThat(properties.parseRetryPolicy(retryPolicyStr) instanceof DefaultRetryPolicy).isTrue();

		retryPolicyStr = "DowngradingConsistencyRetryPolicy";
		assertThat(properties.parseRetryPolicy(retryPolicyStr) instanceof DowngradingConsistencyRetryPolicy).isTrue();

		retryPolicyStr = "FallthroughRetryPolicy";
		assertThat(properties.parseRetryPolicy(retryPolicyStr) instanceof FallthroughRetryPolicy).isTrue();
	}

	@Test
	public void reconnectionPolicyParsingTest() throws Exception {
		CassandraProperties properties = new CassandraProperties();
		String retryPolicyStr = "ConstantReconnectionPolicy((long)10)";
		assertThat(properties.parseReconnectionPolicy(retryPolicyStr) instanceof ConstantReconnectionPolicy).isTrue();

		retryPolicyStr = "ExponentialReconnectionPolicy((long)10,(Long)100)";
		assertThat(properties.parseReconnectionPolicy(retryPolicyStr) instanceof ExponentialReconnectionPolicy).isTrue();
	}

	@Test(expected = ClassNotFoundException.class)
	public void loadBalancingPolicyParsingFailTest() throws Exception {
		CassandraProperties properties = new CassandraProperties();
		String lbPolicyStr = "fakeLbPolicy()";
		assertThat(properties.parseLbPolicy(lbPolicyStr)).isNull();
	}

	@Test(expected = ClassNotFoundException.class)
	public void reconnectionPolicyParsingFailTest() throws Exception {
		CassandraProperties properties = new CassandraProperties();
		String lbPolicyStr = "fakeReconnectionPolicy()";
		assertThat(properties.parseReconnectionPolicy(lbPolicyStr)).isNull();
	}

	@Test(expected = ClassNotFoundException.class)
	public void retryPolicyParsingFailTest() throws Exception {
		CassandraProperties properties = new CassandraProperties();
		String lbPolicyStr = "fakeRetryPolicy";
		assertThat(properties.parseRetryPolicy(lbPolicyStr)).isNull();
	}
}
