package uk.sky.alto.simulacron;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.oss.simulacron.common.cluster.NodeSpec;
import com.datastax.oss.simulacron.common.stubbing.CloseType;
import com.datastax.oss.simulacron.common.stubbing.Prime;
import com.datastax.oss.simulacron.server.BoundNode;
import com.datastax.oss.simulacron.server.RejectScope;
import com.datastax.oss.simulacron.server.Server;
import org.junit.Test;

import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.rows;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;
import static com.datastax.oss.simulacron.driver.SimulacronDriverSupport.defaultBuilder;
import static org.junit.Assert.assertEquals;

// https://github.com/datastax/simulacron/tree/master/doc/java_api#disabling-and-enabling-acceptance-of-connections
public class ConnectionErrors {

    private Prime prime = when("SELECT * FROM table")
            .then(rows()
                    .row("name", "michael", "age", 28)
                    .columnTypes("name", "varchar", "age", "int"))
            .build();

    @Test
    public void simulateCassandraProcessUnresponsive() {
        Server server = Server.builder().build();

        try (BoundNode node = server.register(NodeSpec.builder().build())) {
            // successful connect to cluster
            defaultBuilder(node).build().connect();
            // stop cassandra from accepting new sessions - STOP & UNBIND are 2 other reject scopes available
            node.rejectConnections(0, RejectScope.REJECT_STARTUP);
            // should fail to initialise session
            defaultBuilder(node).build().connect();
        }
    }

    @Test
    public void simulateCassandraProcessUnresponsive_thenHealing() {
        Server server = Server.builder().build();

        try (BoundNode node = server.register(NodeSpec.builder().build())) {
            node.prime(prime);

            // stop cassandra from accepting new sessions - STOP & UNBIND are 2 other reject scopes available
            node.rejectConnections(0, RejectScope.REJECT_STARTUP);
            // should fail to initialise session
            try {
                defaultBuilder(node).build().connect();
            } catch (Throwable ignored) {
            }

            // start cassandra accepting new sessions
            node.acceptConnections();
            Session workingSession = defaultBuilder(node).build().connect();

            Row row = workingSession.execute("SELECT * FROM table").one();

            assertEquals("michael", row.getString("name"));
            assertEquals(28, row.getInt("age"));
        }
    }

    /**
     * DISCONNECT - Simply disconnects the connection.
     */
    @Test
    public void closeSpecificConnections() {
        Server server = Server.builder().build();

        try (BoundNode node = server.register(NodeSpec.builder().build())) {
            node.prime(prime);

            Session session = defaultBuilder(node).build().connect();
            node.closeConnections(CloseType.DISCONNECT);

            Row row = session.execute("SELECT * FROM table").one();

            assertEquals("michael", row.getString("name"));
            assertEquals(28, row.getInt("age"));
        }
    }

    /**
     * SHUTDOWN_READ - Prevents the socket from being able to read any inbound data. Good for testing things like heartbeat failures.
     */
    @Test
    public void closeConnectionFromReadingData() {
        Server server = Server.builder().build();

        try (BoundNode node = server.register(NodeSpec.builder().build())) {
            node.prime(prime);

            Session session = defaultBuilder(node).build().connect();
            node.closeConnections(CloseType.SHUTDOWN_READ);

            Row row = session.execute("SELECT * FROM table").one();

            assertEquals("michael", row.getString("name"));
            assertEquals(28, row.getInt("age"));
        }
    }

    /**
     * SHUTDOWN_WRITE - Prevents the socket from being able to write any outbound data. This will typically close the connection so does not have much use over disconnect.
     */
    @Test
    public void closeConnectionFromWritingData() {
        Server server = Server.builder().build();

        try (BoundNode node = server.register(NodeSpec.builder().build())) {
            node.prime(prime);

            Session session = defaultBuilder(node).build().connect();
            node.closeConnections(CloseType.SHUTDOWN_WRITE);

            Row row = session.execute("SELECT * FROM table").one();

            assertEquals("michael", row.getString("name"));
            assertEquals(28, row.getInt("age"));
        }
    }
}
