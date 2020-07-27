package uk.sky.alto.simulacron;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.oss.simulacron.common.cluster.NodeSpec;
import com.datastax.oss.simulacron.common.stubbing.Prime;
import com.datastax.oss.simulacron.server.BoundNode;
import com.datastax.oss.simulacron.server.Server;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.*;
import static com.datastax.oss.simulacron.driver.SimulacronDriverSupport.defaultBuilder;

public class SlowQueries {

    @Test
    public void slowReadQuery() {
        Server server = Server.builder().build();

        try (BoundNode node = server.register(NodeSpec.builder().build())) {
            Prime prime = when("SELECT * FROM table")
                    .then(rows()
                            .row("name", "michael", "age", 28)
                            .columnTypes("name", "varchar", "age", "int"))
                    .delay(2, TimeUnit.SECONDS)
                    .build();

            node.prime(prime);

            Session session = defaultBuilder(node).build().connect();
            session.execute("SELECT * FROM table").one();
        }
    }

    @Test
    public void slowWriteQuery() {
        Server server = Server.builder().build();

        try (BoundNode node = server.register(NodeSpec.builder().build())) {
            Prime prime = when("INSERT INTO table(key, value) VALUES('abc', 'def')")
                    .then(noRows())
                    .delay(2, TimeUnit.SECONDS)
                    .build();

            node.prime(prime);

            Session session = defaultBuilder(node).build().connect();
            session.execute("INSERT INTO table(key, value) VALUES('abc', 'def')").one();
        }
    }

    @Test
    public void timeoutReadQuery() {
        Server server = Server.builder().build();

        try (BoundNode node = server.register(NodeSpec.builder().build())) {
            Prime prime = when("SELECT * FROM table")
                    .then(rows()
                            .row("name", "michael", "age", 28)
                            .columnTypes("name", "varchar", "age", "int"))
                    .delay(10, TimeUnit.SECONDS)
                    .build();

            node.prime(prime);

            Session session = defaultBuilder(node)
                    .withSocketOptions(
                            new SocketOptions()
                                    .setReadTimeoutMillis(2000)).build().connect();
            session.execute("SELECT * FROM table").one();
        }
    }

    @Test
    public void timeoutWriteQuery() {
        Server server = Server.builder().build();

        try (BoundNode node = server.register(NodeSpec.builder().build())) {
            Prime prime = when("INSERT INTO table(key, value) VALUES('abc', 'def')")
                    .then(noRows())
                    .delay(10, TimeUnit.SECONDS)
                    .build();

            node.prime(prime);

            Session session = defaultBuilder(node)
                    .withSocketOptions(
                            new SocketOptions()
                                    .setReadTimeoutMillis(2000)).build().connect();
            session.execute("INSERT INTO table(key, value) VALUES('abc', 'def')").one();
        }
    }
}
