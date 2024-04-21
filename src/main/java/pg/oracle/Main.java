package pg.oracle;

import oracle.pg.rdbms.GraphServer;
import oracle.pgx.api.*;

import java.io.File;
import java.sql.DriverManager;

public class Main {
    static boolean        PGX_RELOAD;
    static String         PGX_LABEL_DIR;
    static File           PGX_STOP_PGM_LABEL;
    static String         PGX_URL;
    static String         PGX_USERNAME;
    static String         PGX_PASSWORD;
    static String[]       PGX_GRAPH_NAMES;
    static String         PGX_JDBC_URL;
    static ServerInstance PGX_INSTANCE;
    static Thread[]       PGX_THREADS;
    static long           PGX_TOTAL_SYNC_TIME = 0;
    static long           PGX_TOTAL_NUMBER_OF_SYNCS = 0;
    static long           PGX_NUMBER_OF_THREADS = 0;
    static long           PGX_UPTIME = System.currentTimeMillis();

    public static void initApp() throws Exception {
        PGX_RELOAD         = Boolean.parseBoolean(System.getenv("PGX_RELOAD")
                                                        .replace("\"",""));
        PGX_URL            = System.getenv("PGX_URL")
                                   .replace("\"","");
        PGX_LABEL_DIR      = System.getenv("PGX_LABEL_DIR")
                                   .replace("\"","");
        PGX_STOP_PGM_LABEL = new File(Main.PGX_LABEL_DIR
                                      +System.getProperty("file.separator")
                                      +"pgm.stoplabel");
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
        for (int i = 0; i < PGX_GRAPH_NAMES.length; i++ ) {
            PGX_THREADS[i] = new Thread(new GraphThread(PGX_GRAPH_NAMES[i]));
            PGX_THREADS[i].start();
        }

    }

    public static void main(String[] args) {
        try {
            System.out.println("Initializing PropertyGraphManager...");
            initApp();
            System.out.print("Waiting for all threads to finish their initialization processes.");
            while (PGX_NUMBER_OF_THREADS != PGX_THREADS.length) {
                System.out.print(".");
                Thread.sleep(1000);
            }
            System.out.println("\nInitialization completed successfully.");
            System.out.println("Number of graphs/threads : "+PGX_NUMBER_OF_THREADS);
            while (PGX_NUMBER_OF_THREADS > 0)
                Thread.sleep(1000);
            if ( PGX_STOP_PGM_LABEL.exists() )
                PGX_STOP_PGM_LABEL.delete();
            PGX_UPTIME = System.currentTimeMillis() - PGX_UPTIME;
            long seconds = PGX_UPTIME / 1000;
            long minutes = seconds / 60;
            long hours = minutes / 60;
            long days = hours / 24;
            String time = days + " days, " + hours % 24 + " hours, " + minutes % 60 + " minutes, " + seconds % 60 + " seconds";
            System.out.println("All threads stopped. PropertyGraphManager stopped in clean mode.");
            System.out.println("Total number of synchronization threads             : "+PGX_THREADS.length);
            System.out.println("Total number of synchronizations in all threads     : "+PGX_TOTAL_NUMBER_OF_SYNCS);
            System.out.println("Total time spent on synchronizations in all threads : "+PGX_TOTAL_SYNC_TIME);
            System.out.println("Uptime : "+time);
        }
        catch(Exception e) {e.printStackTrace();}
    }
}