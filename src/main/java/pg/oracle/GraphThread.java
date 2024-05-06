package pg.oracle;

import oracle.jdbc.driver.parser.Parser;
import oracle.pgx.api.*;
import oracle.pgx.config.GraphConfigFactory;
import oracle.pgx.config.PartitionedGraphConfig;

import java.io.File;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

public class GraphThread implements Runnable {
    private String graphName;
    private Connection dbConnection;
    private PgxSession pgxConnection;
    private PgxGraph graph;
    private Synchronizer synchronizer;
    private File syncLabel, stopLabel;
    private String configFile;
    private boolean preloaded  = false;
    private long numOfSyncs = 0,
                 syncTime   = 0;

    public GraphThread(String graphName) throws Exception {
        Util.printMessage("GraphThread","GraphThread",graphName,"Graph initialization started");
        this.graphName  = graphName;
        this.syncLabel  = new File(Main.PGX_LABEL_DIR +System.getProperty("file.separator")+this.graphName+".synclabel");
        this.stopLabel  = new File(Main.PGX_LABEL_DIR +System.getProperty("file.separator")+this.graphName+".stoplabel");
        this.configFile = Main.PGX_CONFIG_DIR+System.getProperty("file.separator")+this.graphName+".config.json";
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
        List graphs = pgxConnection.getGraphs(Namespace.PUBLIC);
        if (graphs.contains(this.graphName))
            preloaded = true;
        if ( this.preloaded )
            this.graph = pgxConnection.getGraph(Namespace.PUBLIC,this.graphName);
        else {
            this.graph = this.pgxConnection.readGraphByName(this.graphName, GraphSource.PG_VIEW);
            this.graph.publishWithSnapshots(VertexProperty.ALL, EdgeProperty.ALL);
            this.graph.pin();
            PrintWriter configWriter = new PrintWriter(this.configFile);
            configWriter.write(this.graph.getConfig().toString());
            configWriter.close();
        }
        this.synchronizer  = new Synchronizer.Builder<FlashbackSynchronizer>()
                .setType(FlashbackSynchronizer.class)
                .setGraph(this.graph)
                .setConnection(this.dbConnection)
                .setGraphConfiguration(GraphConfigFactory.forPartitioned()
                                                         .fromFilePath(this.configFile))
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

    public void synchronize() throws Exception {
        long startMs, execMs;
        Util.printMessage("GraphThread","run",this.graphName,"Graph synchronization started.");
        startMs = System.currentTimeMillis();
        this.graph = this.synchronizer.sync();
        if (syncLabel.exists())
            syncLabel.delete();
        execMs = System.currentTimeMillis() - startMs;
        Util.printMessage("GraphThread","run",this.graphName,"Graph synchronized successfully.");
        Util.printMessage("GraphThread","run",this.graphName,this.graph.toString());
        Util.printMessage("GraphThread","run",this.graphName,"Synchronization time (ms) : " + execMs + "\n");
        this.numOfSyncs++;
        this.syncTime += execMs;
    }

    public void run() {
        long startMs, execMs;
        boolean continueRun = true;
        if (Main.PGX_EXECUTION_MODE.equals("BACKGROUND")) {
            while (continueRun) {
                try {
                    while (!syncLabel.exists() &&
                           !stopLabel.exists() &&
                           !Main.PGX_STOP_PGM_LABEL.exists())
                        Thread.sleep(1000);
                    if (syncLabel.exists())
                        synchronize();
                    else {
                        continueRun = false;
                        this.graph.destroy();
                        this.dbConnection.close();
                        this.pgxConnection.close();
                        if (stopLabel.exists())
                            stopLabel.delete();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    continueRun = false;
                }
            }
        }
        else if (Main.PGX_EXECUTION_MODE.equals("SINGLE_RUN") && this.preloaded) {
            try {
                this.synchronize();
            }
            catch (Exception e) {e.printStackTrace();}
        }
        if (Main.PGX_EXECUTION_MODE.equals("BACKGROUND")) {
            Util.printMessage("GraphThread", "run", this.graphName, "Synchronization thread for graph " + this.graphName + " stopped successfully.");
            Util.printMessage("GraphThread", "run", this.graphName, "Total number of synchronizations : " + this.numOfSyncs);
            Util.printMessage("GraphThread", "run", this.graphName, "Total synchronization time (ms)  : " + this.syncTime);
            Util.printMessage("GraphThread", "run", this.graph.toString());
        }
        else {
            if ( this.preloaded )
                Util.printMessage("GraphThread", "run", this.graphName, "Graph " + this.graphName + " synchronized successfully.");
            else
                Util.printMessage("GraphThread", "run", this.graphName, "Graph " + this.graphName + " loaded successfully.");
            Util.printMessage("GraphThread", "run", this.graphName, "Execution time (ms)  : " + this.syncTime);
            Util.printMessage("GraphThread", "run", this.graph.toString());
        }
        Main.PGX_NUMBER_OF_THREADS--;
        Main.PGX_TOTAL_SYNC_TIME += this.syncTime;
        Main.PGX_TOTAL_NUMBER_OF_SYNCS += this.numOfSyncs;
    }
}
