package uk.sky.alto.simulacron;

import com.datastax.driver.core.Session;
import com.datastax.oss.simulacron.common.cluster.NodeSpec;
import com.datastax.oss.simulacron.common.codec.ConsistencyLevel;
import com.datastax.oss.simulacron.common.codec.RequestFailureReason;
import com.datastax.oss.simulacron.common.stubbing.Prime;
import com.datastax.oss.simulacron.common.stubbing.PrimeDsl;
import com.datastax.oss.simulacron.server.BoundNode;
import com.datastax.oss.simulacron.server.Server;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.net.InetAddress;
import java.util.Map;

import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.*;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;
import static com.datastax.oss.simulacron.driver.SimulacronDriverSupport.defaultBuilder;

public class ReadErrors {

    @Test
    public void unauthenticated() {
        Server server = Server.builder().build();

        try (BoundNode node = server.register(NodeSpec.builder().build())) {
            Prime prime = when("SELECT * FROM table")
                    .then(authenticationError("not authenticated"))
                    .build();

            node.prime(prime);

            Session session = defaultBuilder(node).build().connect();
            session.execute("SELECT * FROM table").one();
        }
    }

    @Test
    public void unauthorised() {
        Server server = Server.builder().build();

        try (BoundNode node = server.register(NodeSpec.builder().build())) {
            Prime prime = when("SELECT * FROM table")
                    .then(unauthorized("not authorised"))
                    .build();

            node.prime(prime);

            Session session = defaultBuilder(node).build().connect();
            session.execute("SELECT * FROM table").one();
        }
    }

    @Test
    public void readFailure() {
        Server server = Server.builder().build();

        try (BoundNode node = server.register(NodeSpec.builder().build())) {
            Map<InetAddress, RequestFailureReason> failureReasonByEndpoint = ImmutableMap.of(node.inet(), RequestFailureReason.READ_TOO_MANY_TOMBSTONES);
            Prime prime = when("SELECT * FROM table")
                    .then(PrimeDsl.readFailure(ConsistencyLevel.LOCAL_QUORUM, 1, 2, failureReasonByEndpoint, true))
                    .build();

            node.prime(prime);

            Session session = defaultBuilder(node).build().connect();
            session.execute("SELECT * FROM table").one();
        }
    }

    @Test
    public void readTimeout() {
        Server server = Server.builder().build();

        try (BoundNode node = server.register(NodeSpec.builder().build())) {
            Prime prime = when("SELECT * FROM table")
                    .then(PrimeDsl.readTimeout(ConsistencyLevel.LOCAL_QUORUM, 1, 2, true))
                    .build();

            node.prime(prime);

            Session session = defaultBuilder(node).build().connect();
            session.execute("SELECT * FROM table").one();
        }
    }

    @Test
    public void invalidQuery() {
        Server server = Server.builder().build();

        try (BoundNode node = server.register(NodeSpec.builder().build())) {
            Prime prime = when("SELECT * FROM table")
                    .then(invalid("invalid"))
                    .build();

            node.prime(prime);

            Session session = defaultBuilder(node).build().connect();
            session.execute("SELECT * FROM table").one();
        }
    }

    @Test
    public void serverError() {
        Server server = Server.builder().build();

        try (BoundNode node = server.register(NodeSpec.builder().build())) {
            Prime prime = when("SELECT * FROM table")
                    .then(PrimeDsl.serverError("serverError"))
                    .build();

            node.prime(prime);

            Session session = defaultBuilder(node).build().connect();
            session.execute("SELECT * FROM table").one();
        }
    }
}
