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
    private File syncLabel, stopLabel;
    private long numOfSyncs = 0,
                 syncTime   = 0;
    public GraphThread(String graphName) throws Exception {
        Util.printMessage("GraphThread","GraphThread",graphName,"Graph initialization started");
        this.graphName = graphName;
        this.syncLabel = new File(Main.PGX_LABEL_DIR+System.getProperty("file.separator")+this.graphName+".synclabel");
        this.stopLabel = new File(Main.PGX_LABEL_DIR+System.getProperty("file.separator")+this.graphName+".stoplabel");
        if (syncLabel.exists() || stopLabel.exists()) {
            Util.printMessage("GraphThread","GraphThread",this.graphName,"Previous shutdown was not clean, cleaning the thread.");
            if (syncLabel.exists())
                syncLabel.delete();
            if (stopLabel.exists())
                stopLabel.delete();
        }
        Util.printMessage("GraphThread","GraphThread",this.graphName,"Thread is clean.");
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
        Util.printMessage("GraphThread","GraphThread",this.graphName,"Graph initialized successfully.\n");
    }

    public long getNumOfSyncs() {
        return this.numOfSyncs;
    }

    public long getSyncTime () {
        return this.syncTime;
    }

    public PgxGraph getGraph() {
        return this.graph;
    }

    public void run() {
        long startMs, execMs;
        boolean continueRun = true;
        while (continueRun) {
            try {
                while (!syncLabel.exists() &&
                       !stopLabel.exists() &&
                       !Main.PGX_STOP_PGM_LABEL.exists())
                    Thread.sleep(1000);
                if (syncLabel.exists()) {
                    Util.printMessage("GraphThread","run",this.graphName,"Graph synchronization started.");
                    startMs = System.currentTimeMillis();
                    this.graph = this.synchronizer.sync();
                    syncLabel.delete();
                    execMs = System.currentTimeMillis() - startMs;
                    Util.printMessage("GraphThread","run",this.graphName,"Graph synchronized successfully.");
                    Util.printMessage("GraphThread","run",this.graphName,this.graph.toString());
                    Util.printMessage("GraphThread","run",this.graphName,"Synchronization time (ms) : " + execMs + "\n");
                    this.numOfSyncs++;
                    this.syncTime += execMs;
                } else if (stopLabel.exists()) {
                    continueRun = false;
                    this.graph.destroy();
                    this.dbConnection.close();
                    this.pgxConnection.close();
                    stopLabel.delete();
                } else {
                    continueRun = false;
                    this.graph.destroy();
                    this.dbConnection.close();
                    this.pgxConnection.close();
                }
            }
            catch (Exception e) {
                e.printStackTrace();
                continueRun = false;
            }
        }
        Util.printMessage("GraphThread","run",this.graphName,"Synchronization thread for graph "+this.graphName+" stopped successfully.");
        Util.printMessage("GraphThread","run",this.graphName,"Total number of synchronizations : "+this.numOfSyncs);
        Util.printMessage("GraphThread","run",this.graphName,"Total synchronization time (ms)  : "+this.syncTime);
        Main.PGX_NUMBER_OF_THREADS--;
        Main.PGX_TOTAL_SYNC_TIME += this.syncTime;
        Main.PGX_TOTAL_NUMBER_OF_SYNCS += this.numOfSyncs;
    }
}
