package pg.oracle;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Util {

    public static String getFormattedTime(long timeInMS) {
        long seconds = timeInMS / 1000;
        long minutes = seconds / 60;
        long hours = minutes / 60;
        long days = hours / 24;
        return days + "d, " + hours % 24 + "h, " + minutes % 60 + "m, " + seconds % 60 + "s";
    }

    public static void printMessage(String className,
                                    String methodName,
                                    String message) {
        System.out.println(
                new SimpleDateFormat("YYYY-MM-dd HH:mm:ss:SSS").format(
                        new Date(System.currentTimeMillis())
                )+
                " [" +className+"."+methodName+"] "+
                message
        );
    }

    public static void printMessage(String className,
                                    String methodName,
                                    String objectName,
                                    String message) {
        System.out.println(
                new SimpleDateFormat("YYYY-MM-DD HH:mm:ss:SSS").format(
                        new Date(System.currentTimeMillis())
                )+
                " ["+className+"."+methodName+"] "+
                " ["+objectName+"] "+
                message
        );
    }
}
