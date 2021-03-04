package uk.sky.alto.simulacron;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.NodeSpec;
import com.datastax.oss.simulacron.common.stubbing.Prime;
import com.datastax.oss.simulacron.server.BoundCluster;
import com.datastax.oss.simulacron.server.BoundNode;
import com.datastax.oss.simulacron.server.Server;
import org.junit.Test;

import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.rows;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;
import static com.datastax.oss.simulacron.driver.SimulacronDriverSupport.defaultBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SelectQuery {

    private Prime prime = when("SELECT * FROM table")
            .then(rows()
                    .row("name", "michael", "age", 28)
                    .columnTypes("name", "varchar", "age", "int"))
            .build();

    @Test
    public void happyCase() {
        Server server = Server.builder().build();

        Prime overridenPrime = when("SELECT * FROM table")
                .then(rows()
                        .row("name", "luke", "age", 25)
                        .columnTypes("name", "varchar", "age", "int"))
                .build();

        try (BoundCluster cluster = server.register(ClusterSpec.builder().withNodes(1).build())) {
            // can clean primes and logs using
            //  node.clearPrimes();
            //  node.clearLogs();

            cluster.prime(prime);
            cluster.node(0).prime(overridenPrime);

            Session session = defaultBuilder(cluster).build().connect();
            Row row = session.execute("SELECT * FROM table").one();

            assertEquals("michael", row.getString("name"));
            assertEquals(28, row.getInt("age"));

            System.out.println("Query Log:");
            cluster.getLogs().getQueryLogs().forEach(System.out::println);
        }
    }

    @Test
    public void happyCase_overrideCassandraVersion() {
        Server server = Server.builder().build();

        // default cassandra version is "3.0.12"
        NodeSpec nodeSpec = NodeSpec.builder().withCassandraVersion("2.1.10").build();
        try (BoundNode node = server.register(nodeSpec)) {
            node.prime(prime);

            Session session = defaultBuilder(node).build().connect();
            Row row = session.execute("SELECT * FROM table").one();

            assertEquals("michael", row.getString("name"));
            assertEquals(28, row.getInt("age"));
            assertTrue(verifyQuery("SELECT * FROM table", node));
        }
    }

    @Test
    public void happyCase_overrideDseVersion() {
        Server server = Server.builder().build();

        // default cassandra version is "3.0.12"
        NodeSpec nodeSpec = NodeSpec.builder().withDSEVersion("6.8").build();
        try (BoundNode node = server.register(nodeSpec)) {
            node.prime(prime);

            Session session = defaultBuilder(node).build().connect();
            Row row = session.execute("SELECT * FROM table").one();

            assertEquals("michael", row.getString("name"));
            assertEquals(28, row.getInt("age"));
            assertTrue(verifyQuery("SELECT * FROM table", node));
        }
    }

    private static boolean verifyQuery(String query, BoundNode node) {
        // verify query was made by checking through query logs
        return node.getLogs().getQueryLogs().stream()
                .filter(queryLog -> queryLog.getFrame().message instanceof Query)
                .anyMatch(queryLog -> ((Query) queryLog.getFrame().message).query.equals(query));
    }


}
