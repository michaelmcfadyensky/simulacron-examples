package uk.sky.alto.simulacron;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.oss.simulacron.common.cluster.NodeSpec;
import com.datastax.oss.simulacron.common.stubbing.Prime;
import com.datastax.oss.simulacron.server.BoundNode;
import com.datastax.oss.simulacron.server.Server;
import org.junit.Test;

import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.rows;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;
import static com.datastax.oss.simulacron.driver.SimulacronDriverSupport.defaultBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class QueryMismatches {

    private Prime prime = when("SELECT * FROM table")
            .then(rows()
                    .row("name", "michael", "age", 28)
                    .columnTypes("name", "varchar", "age", "int"))
            .build();

    /**
     * Query matching is case sensitive, even for keywords
     */
    @Test
    public void queryMismatch_casing() {
        Server server = Server.builder().build();

        try (BoundNode node = server.register(NodeSpec.builder().build())) {
            node.prime(prime);

            Session session = defaultBuilder(node).build().connect();
            ResultSet resultSet = session.execute("SELECT * from table");

            assertFalse(resultSet.isExhausted());
            Row row = resultSet.one();
            assertEquals("michael", row.getString("name"));
            assertEquals(28, row.getInt("age"));
        }
    }

    /**
     * Query matching compares exact strings, including delimiters
     */
    @Test
    public void queryMismatch_semicolon() {
        Server server = Server.builder().build();

        try (BoundNode node = server.register(NodeSpec.builder().build())) {
            node.prime(prime);

            Session session = defaultBuilder(node).build().connect();
            ResultSet resultSet = session.execute("SELECT * from table;");

            assertFalse(resultSet.isExhausted());
            Row row = resultSet.one();
            assertEquals("michael", row.getString("name"));
            assertEquals(28, row.getInt("age"));
        }
    }

    /**
     * BE AWARE: If no prime is found, a empty result set will be returned and no errors will be logged
     */
    @Test
    public void queryMismatch_doesNotThrowErrorOnMismatch() {
        Server server = Server.builder().build();

        try (BoundNode node = server.register(NodeSpec.builder().build())) {
            node.prime(prime);

            Session session = defaultBuilder(node).build().connect();
            ResultSet resultSet = session.execute("INSERT INTO table (key,value) VALUES('abd', 'def')");

            System.out.println(resultSet);
        }
    }
}
