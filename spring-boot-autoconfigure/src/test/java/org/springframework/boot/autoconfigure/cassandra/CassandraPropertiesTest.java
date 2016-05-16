package org.springframework.boot.autoconfigure.cassandra;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import com.datastax.driver.core.policies.LatencyAwarePolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

public class CassandraPropertiesTest {

	@Test
	public void loadBalancingPolicyParsingTest() throws Exception
    {
		CassandraProperties properties = new CassandraProperties();
    	String lbPolicyStr = "RoundRobinPolicy()";
    	assertTrue(properties.parseLbPolicy(lbPolicyStr) instanceof RoundRobinPolicy);

    	lbPolicyStr = "RoundRobinPolicy";
    	assertTrue(properties.parseLbPolicy(lbPolicyStr) instanceof RoundRobinPolicy);

    	lbPolicyStr = "TokenAwarePolicy(RoundRobinPolicy())";
    	assertTrue(properties.parseLbPolicy(lbPolicyStr) instanceof TokenAwarePolicy);

    	lbPolicyStr = "DCAwareRoundRobinPolicy(\"dc1\")";
    	assertTrue(properties.parseLbPolicy(lbPolicyStr) instanceof DCAwareRoundRobinPolicy);

    	lbPolicyStr = "TokenAwarePolicy(DCAwareRoundRobinPolicy(\"dc1\"))";
    	assertTrue(properties.parseLbPolicy(lbPolicyStr) instanceof TokenAwarePolicy);    	

    	lbPolicyStr = "LatencyAwarePolicy(TokenAwarePolicy(RoundRobinPolicy()),(double) 10.5,(long) 1,(long) 10,(long)1,10)";    	
    	assertTrue(properties.parseLbPolicy(lbPolicyStr) instanceof LatencyAwarePolicy);    	
    }
	
	
    @Test
    public void retryPolicyParsingTest() throws Exception
    {
    	CassandraProperties properties = new CassandraProperties();
    	String retryPolicyStr = "DefaultRetryPolicy";
    	assertTrue(properties.parseRetryPolicy(retryPolicyStr) instanceof DefaultRetryPolicy);

    	retryPolicyStr = "DowngradingConsistencyRetryPolicy";
    	assertTrue(properties.parseRetryPolicy(retryPolicyStr) instanceof DowngradingConsistencyRetryPolicy);

    	retryPolicyStr = "FallthroughRetryPolicy";
    	assertTrue(properties.parseRetryPolicy(retryPolicyStr) instanceof FallthroughRetryPolicy);

    	    	

    }
    
    
    @Test
    public void reconnectionPolicyParsingTest() throws Exception
    {
    	CassandraProperties properties = new CassandraProperties();
    	String retryPolicyStr = "ConstantReconnectionPolicy((long)10)";
    	assertTrue(properties.parseReconnectionPolicy(retryPolicyStr) instanceof ConstantReconnectionPolicy);

    	retryPolicyStr = "ExponentialReconnectionPolicy((long)10,(Long)100)";
    	assertTrue(properties.parseReconnectionPolicy(retryPolicyStr) instanceof ExponentialReconnectionPolicy);
    }
    
    @Test(expected=ClassNotFoundException.class)
	public void loadBalancingPolicyParsingFailTest() throws Exception
    {
		CassandraProperties properties = new CassandraProperties();
    	String lbPolicyStr = "fakeLbPolicy()";
    	assertNull(properties.parseLbPolicy(lbPolicyStr));
    }
    
    @Test(expected=ClassNotFoundException.class)
	public void reconnectionPolicyParsingFailTest() throws Exception
    {
		CassandraProperties properties = new CassandraProperties();
    	String lbPolicyStr = "fakeReconnectionPolicy()";
    	assertNull(properties.parseReconnectionPolicy(lbPolicyStr));
    }
    
    @Test(expected=ClassNotFoundException.class)
	public void retryPolicyParsingFailTest() throws Exception
    {
		CassandraProperties properties = new CassandraProperties();
    	String lbPolicyStr = "fakeRetryPolicy";
    	assertNull(properties.parseRetryPolicy(lbPolicyStr));
    }
	
}
