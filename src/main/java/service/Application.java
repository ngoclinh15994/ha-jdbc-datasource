package service;

import net.sf.hajdbc.SimpleDatabaseClusterConfigurationFactory;
import net.sf.hajdbc.balancer.load.LoadBalancerFactory;
import net.sf.hajdbc.balancer.roundrobin.RoundRobinBalancerFactory;
import net.sf.hajdbc.balancer.simple.SimpleBalancerFactory;
import net.sf.hajdbc.cache.simple.SimpleDatabaseMetaDataCacheFactory;
import net.sf.hajdbc.dialect.postgresql.PostgreSQLDialectFactory;
import net.sf.hajdbc.distributed.jgroups.JGroupsCommandDispatcherFactory;
import net.sf.hajdbc.durability.fine.FineDurabilityFactory;
import net.sf.hajdbc.sql.DataSource;
import net.sf.hajdbc.sql.DataSourceDatabase;
import net.sf.hajdbc.sql.DriverDatabase;
//import net.sf.hajdbc.sql.DriverDatabaseClusterConfiguration;
import net.sf.hajdbc.sql.DriverDatabaseClusterConfiguration;
import net.sf.hajdbc.sql.DataSourceDatabaseClusterConfiguration;
import net.sf.hajdbc.state.simple.SimpleStateManagerFactory;
import net.sf.hajdbc.state.sql.SQLStateManagerFactory;


import java.sql.*;
import java.util.Arrays;

public class Application {
    public DataSource getHaConnection() throws SQLException {

        DataSourceDatabase db3 = new DataSourceDatabase();
        db3.setId("db3");
        db3.setLocation("org.postgresql.ds.PGSimpleDataSource");
        db3.setProperty("url", "jdbc:postgresql://localhost:5433/aDB");
        db3.setProperty("user", "postgres");
        db3.setProperty("password", "");
        db3.setWeight(1);
        db3.setUser("postgres");
        db3.setPassword("");

        DataSourceDatabase db2 = new DataSourceDatabase();
        db2.setId("db2");
        db2.setLocation("org.postgresql.ds.PGSimpleDataSource");
        db2.setProperty("url", "jdbc:postgresql://localhost:5432/bDB");
        db2.setProperty("user", "postgres");
        db2.setProperty("password", "");
        db2.setUser("postgres");
        db2.setPassword("");
        db2.setWeight(2);


// Define the cluster configuration itself
        DataSourceDatabaseClusterConfiguration config = new DataSourceDatabaseClusterConfiguration();
// Specify the database composing this cluster
        config.setDatabases(Arrays.asList(db2, db3));
// Define the dialect
        config.setDialectFactory(new PostgreSQLDialectFactory());
// Don't cache any meta data
        config.setDatabaseMetaDataCacheFactory(new SimpleDatabaseMetaDataCacheFactory());
// Use an in-memory state manager
        config.setStateManagerFactory(new SimpleStateManagerFactory());
// Make the cluster distributable
//        config.setDispatcherFactory(new JGroupsCommandDispatcherFactory());

        config.setBalancerFactory(new SimpleBalancerFactory());  // send request to higher weight DB first

//        config.setSequenceDetectionEnabled(true);

//        config.setDefaultSynchronizationStrategy("full");

//        config.setDurabilityFactory( new FineDurabilityFactory());

// Database cluster is now ready to be used!
        DataSource ds = new DataSource();
        ds.setCluster("cluster");
        ds.setConfigurationFactory(new SimpleDatabaseClusterConfigurationFactory<javax.sql.DataSource, DataSourceDatabase>(config));
        return ds;

    }

    public static void main(String[] args) throws SQLException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        Class.forName("org.postgresql.Driver").newInstance();
        Application application = new Application();
        DataSource dataSource = application.getHaConnection();
        if(dataSource != null){
            Connection connection = dataSource.getConnection();
            ResultSet resultSet = connection.prepareStatement("select * from board").executeQuery();
            while (resultSet.next()) {
                System.out.printf("firt>>>");
                System.out.println(resultSet.getString(1));
                break;
            }

            ResultSet resultSet2 = connection.prepareStatement("select * from board").executeQuery();
            while (resultSet2.next()) {
                System.out.printf("second>>>");
                System.out.println(resultSet2.getString(1));
                break;
            }

            connection.close();
//            System.exit(0);
        }

    }
}
