package pg.oracle;


import oracle.pg.rdbms.GraphServer;
import oracle.pg.rdbms.pgql.PgqlConnection;
import oracle.pg.rdbms.pgql.PgqlStatement;
import oracle.pg.rdbms.pgql.jdbc.PgqlJdbcRdbmsDriver;
import oracle.pgx.api.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URI;
import java.net.http.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Map;
import oracle.pgx.config.*;
import org.json.*;

// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
public class Main {
    static String PGX_APP_EXEC_MODE;
    static boolean PGX_RELOAD;
    static String PGX_LABEL_FILE_NAME;
    static String PGX_URL;
    static String PGX_USERNAME;
    static String PGX_PASSWORD;
    static String PGX_GRAPH_NAME;
    static String PGX_JDBC_URL;
    static Connection PGX_JDBC_CONNECTION;
    static ServerInstance PGX_INSTANCE;
    static PgxSession PGX_SESSION;
    static Synchronizer PGX_SYNCHRONIZER;
    static PgxGraph PGX_GRAPH;
    static GraphConfig PGX_GRAPH_CONFIG;
    static PartitionedGraphConfig PGX_PART_GRAPH_CONFIG;
    static long syncTime = 0;
    public static void initApp() throws Exception {
        PGX_APP_EXEC_MODE  = System.getenv("PGX_APP_EXEC_MODE").replace("\"","");
        //PGX_APP_EXEC_MODE = "INTERACTIVE";
        //PGX_APP_EXEC_MODE = "NOHUP";
        PGX_RELOAD = Boolean.parseBoolean(System.getenv("PGX_RELOAD").replace("\"",""));
        if ( PGX_APP_EXEC_MODE.equals("NOHUP") )
            PGX_LABEL_FILE_NAME = System.getenv("PGX_LABEL_FILE_NAME").replace("\"","");
        else
            PGX_LABEL_FILE_NAME = "N/A";
        PGX_URL            = System.getenv("PGX_URL").replace("\"","");
        PGX_JDBC_URL       = System.getenv("PGX_JDBC_URL").replace("\"","");
        PGX_USERNAME       = System.getenv("PGX_USERNAME").replace("\"","");
        PGX_PASSWORD       = System.getenv("PGX_PASSWORD").replace("\"","");
        PGX_GRAPH_NAME     = System.getenv("PGX_GRAPH_NAME").replace("\"","");
    }

    public static void loadGraph() throws Exception {
        DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
        PGX_JDBC_CONNECTION = DriverManager.getConnection(PGX_JDBC_URL, PGX_USERNAME, PGX_PASSWORD);
        PGX_JDBC_CONNECTION.setAutoCommit(false);
        PGX_INSTANCE        = GraphServer.getInstance(PGX_URL, PGX_USERNAME, PGX_PASSWORD.toCharArray());
        if ( PGX_RELOAD ) {
            PGX_INSTANCE.shutdownEngineNow();
            PGX_INSTANCE.startEngine();
        }
        PGX_SESSION         = PGX_INSTANCE.createSession("my-session");
        PGX_GRAPH           = PGX_SESSION.readGraphByName(PGX_GRAPH_NAME, GraphSource.PG_VIEW);
        PGX_GRAPH_CONFIG    = PGX_GRAPH.getConfig();
        PGX_GRAPH.publishWithSnapshots();
        PGX_PART_GRAPH_CONFIG=GraphConfigFactory.forPartitioned().fromJson(PGX_GRAPH_CONFIG.toString());
        PGX_SYNCHRONIZER = new Synchronizer.Builder<FlashbackSynchronizer>()
                .setType(FlashbackSynchronizer.class)
                .setGraph(PGX_GRAPH)
                .setConnection(PGX_JDBC_CONNECTION)
                .setGraphConfiguration(PGX_PART_GRAPH_CONFIG)
                .build();
    }

    public static void synchronizeGraph() throws Exception {
        long beginTime = System.currentTimeMillis();
        PGX_GRAPH=PGX_SYNCHRONIZER.sync();
        syncTime = (System.currentTimeMillis()-beginTime);
    }

    public static void main(String[] args) {
        String continueExecution = "Y" ;
        try {
            System.out.println("Initializing PropertyGraphManager...");
            initApp();
            System.out.println("PropertyGraphManager initialized succesfully.");
            System.out.println("Execution mode   : "+PGX_APP_EXEC_MODE);
            System.out.println("Label file       : "+PGX_LABEL_FILE_NAME);
            System.out.println("Graph Server URL : "+PGX_URL);
            System.out.println("JDBC URL        : "+PGX_JDBC_URL);
            System.out.println("Graph           : "+PGX_GRAPH_NAME);
            System.out.println("Server Reload   : "+PGX_RELOAD);
            System.out.println("Loading graph "+PGX_GRAPH_NAME+" into PGX Server memory...");
            loadGraph();
            System.out.println("Graph "+PGX_GRAPH_NAME+" loaded and published successfully.");
            System.out.println("Entering the synchronization loop....");
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            if ( PGX_APP_EXEC_MODE.equals("INTERACTIVE") ) {
                do {
                    if (!continueExecution.equalsIgnoreCase("Y") &&
                            !continueExecution.equalsIgnoreCase("N"))
                        System.out.print("Invalid key. Do you want to continue ? (Y/N) ");
                    else if (continueExecution.equalsIgnoreCase("Y")) {
                        synchronizeGraph();
                        System.out.println("Synchronization completed");
                        System.out.println("Number of edges           : " + PGX_GRAPH.getNumEdges());
                        System.out.println("Number of vertices        : " + PGX_GRAPH.getNumVertices());
                        System.out.println("Synchronization time (ms) : "+syncTime);
                        System.out.print("Do you want to continue ? (Y/N) ");
                    }
                    continueExecution = br.readLine();
                }
                while (!continueExecution.equalsIgnoreCase("N"));
                System.out.println("Exiting...");
            }
            else {
                File labelFile = new File(PGX_LABEL_FILE_NAME);
                while (true) {
                    System.out.println("Waiting for the label to start synchronization...");
                    while (!labelFile.exists())
                        Thread.sleep(1000);
                    System.out.println("Starting synchronization.");
                    synchronizeGraph();
                    System.out.println("Synchronization completed.");
                    System.out.println("Number of edges           : " + PGX_GRAPH.getNumEdges());
                    System.out.println("Number of vertices        : " + PGX_GRAPH.getNumVertices());
                    System.out.println("Synchronization time (ms) : "+syncTime);
                    labelFile.delete();
                }
            }
        }
        catch(Exception e) {e.printStackTrace();}
    }
}