package pg.oracle;

import oracle.pg.rdbms.GraphServer;
import oracle.pgx.api.*;
import java.io.File;
import java.sql.DriverManager;

public class Main {
    static String         PGX_EXECUTION_MODE;
    static boolean        PGX_RELOAD;
    static String         PGX_LABEL_DIR;
    static String         PGX_CONFIG_DIR;
    static File           PGX_STOP_PGM_LABEL;
    static String         PGX_URL;
    static String         PGX_USERNAME;
    static String         PGX_PASSWORD;
    static String[]       PGX_GRAPH_NAMES;
    static String         PGX_JDBC_URL;
    static ServerInstance PGX_INSTANCE;
    static Thread[]       PGX_THREADS;
    static GraphThread[]  PGX_GRAPHS;
    static long           PGX_TOTAL_SYNC_TIME = 0;
    static long           PGX_TOTAL_NUMBER_OF_SYNCS = 0;
    static long           PGX_NUMBER_OF_THREADS = 0;
    static long           PGX_STARTTIME = System.currentTimeMillis();
    static File           PGX_DATA_DISP_LABEL;

    public static void initApp() throws Exception {
        PGX_EXECUTION_MODE = System.getenv("PGX_EXECUTION_MODE")
                                   .replace("\"","");
        PGX_RELOAD         = Boolean.parseBoolean(System.getenv("PGX_RELOAD")
                                                       .replace("\"",""));
        //PGX_RELOAD = false;
        PGX_URL            = System.getenv("PGX_URL")
                                   .replace("\"","");
        PGX_LABEL_DIR      = System.getenv("PGX_LABEL_DIR")
                                   .replace("\"","");
        PGX_CONFIG_DIR     = System.getenv("PGX_CONFIG_DIR")
                                   .replace("\"","");
        PGX_STOP_PGM_LABEL = new File(PGX_LABEL_DIR
                                      +System.getProperty("file.separator")
                                      +"pgm.stoplabel");
        PGX_DATA_DISP_LABEL = new File(PGX_LABEL_DIR
                                      +System.getProperty("file.separator")
                                      +"pgm.showlabel");
        PGX_JDBC_URL       = System.getenv("PGX_JDBC_URL")
                                   .replace("\"","");
        PGX_USERNAME       = System.getenv("PGX_USERNAME")
                                   .replace("\"","");
        PGX_PASSWORD       = System.getenv("PGX_PASSWORD")
                                   .replace("\"","");
        PGX_GRAPH_NAMES    = System.getenv("PGX_GRAPH_NAMES")
                                   .replace("\"","").split(",");
        DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
        PGX_INSTANCE        = GraphServer.getInstance(PGX_URL, PGX_USERNAME, PGX_PASSWORD.toCharArray());
        if ( PGX_RELOAD ) {
            PGX_INSTANCE.shutdownEngineNow();
            PGX_INSTANCE.startEngine();
        }
        PGX_THREADS = new Thread[PGX_GRAPH_NAMES.length];
        PGX_GRAPHS  = new GraphThread[PGX_GRAPH_NAMES.length];
        for (int i = 0; i < PGX_GRAPH_NAMES.length; i++ ) {
            PGX_GRAPHS[i]  = new GraphThread(PGX_GRAPH_NAMES[i]);
            PGX_THREADS[i] = new Thread(PGX_GRAPHS[i]);
            PGX_THREADS[i].start();
        }
    }

    static void printSummary() {
        Util.printMessage("Main","main","Number of maintained graphs/threads : " + PGX_THREADS.length);
        Util.printMessage("Main","main","Total sync operations               : " + PGX_TOTAL_NUMBER_OF_SYNCS);
        Util.printMessage("Main","main","Execution mode                      : "+PGX_EXECUTION_MODE);
        Util.printMessage("Main","main","Total sync time                     : " + Util.getFormattedTime(PGX_TOTAL_SYNC_TIME));
        if (PGX_EXECUTION_MODE.equals("BACKGROUND"))
            Util.printMessage("Main","main","Uptime                              : " + Util.getFormattedTime(System.currentTimeMillis() - PGX_STARTTIME)+"\n");
    }

    public static void main(String[] args) {
        try {
            Util.printMessage("Main","main","Initializing PropertyGraphManager...");
            initApp();
            if ( PGX_EXECUTION_MODE.equals("BACKGROUND")) {
                Util.printMessage("Main", "main", "Waiting for all threads to finish their initialization processes.");
                while (PGX_NUMBER_OF_THREADS != PGX_THREADS.length) {
                    System.out.print(".");
                    Thread.sleep(1000);
                }
            }
            System.out.println("\n");
            Util.printMessage("Main","main","Initialization completed successfully.");
            Util.printMessage("Main","main","Graphs : ");
            for (int i = 0; i < PGX_THREADS.length; i++)
                Util.printMessage("Main","main","   " + PGX_GRAPHS[i].getGraph());
            printSummary();
            while (PGX_NUMBER_OF_THREADS > 0) {
                if (PGX_DATA_DISP_LABEL.exists()) {
                    System.out.println("Graphs : ");
                    for (int i = 0; i < PGX_THREADS.length; i++)
                        Util.printMessage("Main","main","   " + PGX_GRAPHS[i].getGraph() +
                                ", #syncs : " + PGX_GRAPHS[i].getNumOfSyncs() +
                                ", sync time : " + Util.getFormattedTime(PGX_GRAPHS[i].getSyncTime()));
                    printSummary();
                    PGX_DATA_DISP_LABEL.delete();
                }
                Thread.sleep(1000);
            }
            Util.printMessage("Main","main","All threads stopped. PropertyGraphManager stopped in clean mode.");
            if (PGX_STOP_PGM_LABEL.exists())
                PGX_STOP_PGM_LABEL.delete();
            printSummary();
        }
        catch(Exception e) {e.printStackTrace();}
    }
}