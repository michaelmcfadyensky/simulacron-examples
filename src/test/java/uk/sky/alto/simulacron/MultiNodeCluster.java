package uk.sky.alto.simulacron;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.stubbing.Prime;
import com.datastax.oss.simulacron.server.BoundCluster;
import com.datastax.oss.simulacron.server.Server;
import org.junit.Test;

import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.rows;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;
import static com.datastax.oss.simulacron.driver.SimulacronDriverSupport.defaultBuilder;
import static org.junit.Assert.assertEquals;

public class MultiNodeCluster {

    /**
     * The following is required to run the following tests. This will add loopbacks for 127.0.0.*
     * for sub in {0..4}; do
     * echo "Opening for 127.0.$sub"
     * for i in {0..255}; do sudo ifconfig lo0 alias 127.0.$sub.$i up; done
     * done
     **/

    private Prime prime = when("SELECT * FROM table")
            .then(rows()
                    .row("name", "michael", "age", 28)
                    .columnTypes("name", "varchar", "age", "int"))
            .build();

    @Test
    public void multiNodeCluster() {
        Server server = Server.builder().build();

        ClusterSpec clusterSpec = ClusterSpec.builder()
                .withNodes(10)
                .build();
        try (BoundCluster cluster = server.register(clusterSpec)) {
            cluster.prime(prime);

            Session session = defaultBuilder(cluster).build().connect();
            ResultSet resultSet = session.execute("SELECT * FROM table");
            Row row = resultSet.one();

            assertEquals("michael", row.getString("name"));
            assertEquals(28, row.getInt("age"));
        }
    }

    @Test
    public void multiNodeMultiDcCluster() {
        Server server = Server.builder().build();

        ClusterSpec clusterSpec = ClusterSpec.builder()
                .withNodes(10, 10)
                .build();
        try (BoundCluster cluster = server.register(clusterSpec)) {
            cluster.prime(prime);

            Session session = defaultBuilder(cluster).build().connect();
            ResultSet resultSet = session.execute("SELECT * FROM table");
            Row row = resultSet.one();

            assertEquals("michael", row.getString("name"));
            assertEquals(28, row.getInt("age"));
        }
    }

    /**
     * By default simulacron creates each DC with the name "dc$i" where $i is an incrementing number starting at 1
     * For example, creating 2 DCs will create them with names "dc1" and "dc2"
     * <p>
     * You can change the value of {@link DCAwareRoundRobinPolicy.Builder#withLocalDc} to force the cassandra driver
     * to connect to a different DC
     */
    @Test
    public void multiNodeMultiDcCluster_singleDcFailing() {
        Server server = Server.builder().build();

        ClusterSpec clusterSpec = ClusterSpec.builder()
                .withNodes(30, 30, 30)
                .build();
        try (BoundCluster cluster = server.register(clusterSpec)) {
            cluster.prime(prime);

            Session session = defaultBuilder(cluster)
                    .withLoadBalancingPolicy(DCAwareRoundRobinPolicy.builder()
                            .withLocalDc("dc3")
                            .build())
                    .build()
                    .connect();

            cluster.dc(1).pauseRead();

            ResultSet resultSet = session.execute("SELECT * FROM table");
            Row row = resultSet.one();

            assertEquals("michael", row.getString("name"));
            assertEquals(28, row.getInt("age"));
        }
    }
}
