package uk.sky.alto.simulacron;

import com.datastax.driver.core.Session;
import com.datastax.oss.simulacron.common.cluster.NodeSpec;
import com.datastax.oss.simulacron.common.stubbing.Prime;
import com.datastax.oss.simulacron.server.BoundNode;
import com.datastax.oss.simulacron.server.Server;
import org.junit.Test;

import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.noRows;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;
import static com.datastax.oss.simulacron.driver.SimulacronDriverSupport.defaultBuilder;
import static org.junit.Assert.assertTrue;

public class InsertQuery {

    private Prime prime = when("INSERT INTO table(key, value) VALUES('abc', 'def')")
            .then(noRows())
            .build();

    @Test
    public void happyCase() {
        Server server = Server.builder().build();

        try (BoundNode node = server.register(NodeSpec.builder().build())) {
            node.prime(prime);

            Session session = defaultBuilder(node).build().connect();
            boolean wasApplied = session.execute("INSERT INTO table(key, value) VALUES('abc', 'def')").wasApplied();

            assertTrue(wasApplied);
        }
    }
}
