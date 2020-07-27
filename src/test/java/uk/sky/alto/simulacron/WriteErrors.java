package uk.sky.alto.simulacron;

import com.datastax.driver.core.Session;
import com.datastax.oss.simulacron.common.cluster.NodeSpec;
import com.datastax.oss.simulacron.common.codec.ConsistencyLevel;
import com.datastax.oss.simulacron.common.codec.RequestFailureReason;
import com.datastax.oss.simulacron.common.codec.WriteType;
import com.datastax.oss.simulacron.common.stubbing.Prime;
import com.datastax.oss.simulacron.common.stubbing.PrimeDsl;
import com.datastax.oss.simulacron.server.BoundNode;
import com.datastax.oss.simulacron.server.Server;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.net.InetAddress;
import java.util.Map;

import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;
import static com.datastax.oss.simulacron.driver.SimulacronDriverSupport.defaultBuilder;

public class WriteErrors {
    @Test
    public void writeFailure() {
        Server server = Server.builder().build();

        try (BoundNode node = server.register(NodeSpec.builder().build())) {
            Map<InetAddress, RequestFailureReason> failureReasonByEndpoint = ImmutableMap.of(node.inet(), RequestFailureReason.READ_TOO_MANY_TOMBSTONES);
            Prime prime = when("INSERT INTO table(key, value) VALUES('abc', 'def')")
                    .then(PrimeDsl.writeFailure(ConsistencyLevel.LOCAL_QUORUM, 1, 2, failureReasonByEndpoint, WriteType.SIMPLE))
                    .build();

            node.prime(prime);

            Session session = defaultBuilder(node).build().connect();
            session.execute("INSERT INTO table(key, value) VALUES('abc', 'def')").one();
        }
    }
    @Test
    public void writeTimeout() {
        Server server = Server.builder().build();

        try (BoundNode node = server.register(NodeSpec.builder().build())) {
            Prime prime = when("INSERT INTO table(key, value) VALUES('abc', 'def')")
                    .then(PrimeDsl.writeTimeout(ConsistencyLevel.LOCAL_QUORUM, 1, 2, WriteType.SIMPLE))
                    .build();

            node.prime(prime);

            Session session = defaultBuilder(node).build().connect();
            session.execute("INSERT INTO table(key, value) VALUES('abc', 'def')").one();
        }
    }
}
