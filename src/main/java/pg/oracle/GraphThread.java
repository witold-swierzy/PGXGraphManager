package pg.oracle;

import oracle.pgx.api.*;
import oracle.pgx.config.GraphConfigFactory;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;

public class GraphThread implements Runnable {
    private String graphName;
    private Connection dbConnection;
    private PgxSession pgxConnection;
    private PgxGraph graph;
    private Synchronizer synchronizer;
    private long numOfSyncs = 0,
                 syncTime   = 0;
    public GraphThread(String graphName) throws Exception {
        System.out.println("Initializing thread for graph : "+graphName);
        this.graphName     = graphName;
        this.dbConnection  = DriverManager.getConnection(Main.PGX_JDBC_URL, Main.PGX_USERNAME, Main.PGX_PASSWORD);
        this.dbConnection.setAutoCommit(false);
        this.pgxConnection = Main.PGX_INSTANCE.createSession(this.graphName);
        this.graph         = this.pgxConnection.readGraphByName(this.graphName, GraphSource.PG_VIEW);
        this.graph.publishWithSnapshots();
        this.synchronizer  = new Synchronizer.Builder<FlashbackSynchronizer>()
                .setType(FlashbackSynchronizer.class)
                .setGraph(this.graph)
                .setConnection(this.dbConnection)
                .setGraphConfiguration(GraphConfigFactory
                        .forPartitioned()
                        .fromJson(this.graph.getConfig().toString()))
                .build();
        Main.PGX_NUMBER_OF_THREADS++;
        System.out.println("Graph "+graphName+" initialized successfully.");
    }

    public void run() {
        long startMs, execMs;
        boolean continueRun = true;
        File syncLabel = new File(Main.PGX_LABEL_DIR+System.getProperty("file.separator")+this.graphName+".synclabel"),
             stopThreadLabel = new File(Main.PGX_LABEL_DIR+System.getProperty("file.separator")+this.graphName+".stoplabel");
        while (continueRun) {
            try {
                while ( !syncLabel.exists() &&
                        !stopThreadLabel.exists() &&
                        !Main.PGX_STOP_PGM_LABEL.exists() )
                    Thread.sleep(1000);
                if (syncLabel.exists()) {
                    System.out.println("Synchronization for graph "+this.graphName+" started.");
                    startMs = System.currentTimeMillis();
                    this.graph = this.synchronizer.sync();
                    syncLabel.delete();
                    execMs = System.currentTimeMillis() - startMs;
                    System.out.println("Graph "+this.graphName+" synchronized successfully.");
                    System.out.println("Synchronization time : "+execMs);
                    System.out.println("Number of vertices   : "+this.graph.getNumVertices());
                    System.out.println("Number of edges      : "+this.graph.getNumEdges());
                    this.numOfSyncs ++;
                    this.syncTime += execMs;
                } else if (stopThreadLabel.exists()) {
                    continueRun = false;
                    this.dbConnection.close();
                    this.pgxConnection.close();
                    stopThreadLabel.delete();
                } else {
                    continueRun = false;
                    this.dbConnection.close ();
                    this.pgxConnection.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
                continueRun = false;
            }
        }
        System.out.println("Synchronization thread for graph "+this.graphName+" stopped succesfully.");
        System.out.println("Total number of synchronizations : "+this.numOfSyncs);
        System.out.println("Total synchronization time (ms)  : "+this.syncTime);
        Main.PGX_NUMBER_OF_THREADS--;
        Main.PGX_TOTAL_SYNC_TIME += this.syncTime;
        Main.PGX_TOTAL_NUMBER_OF_SYNCS += this.numOfSyncs;
    }
}
