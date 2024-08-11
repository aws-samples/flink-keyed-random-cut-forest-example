package software.amazon.flink.example.rcf;

import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import java.util.Properties;

public class ApplicationConfigUtils {

    public static String parseMandatoryString(Properties properties, String propertyName) {
        String propertyStr = properties.getProperty(propertyName);
        Preconditions.checkArgument(!StringUtils.isNullOrWhitespaceOnly(propertyStr), "Missing application parameter '" + propertyName + "'");
        return propertyStr;
    }

    public static double parseMandatoryDouble(Properties properties, String propertyName) {
        String propertyStr = properties.getProperty(propertyName);
        Preconditions.checkArgument(!StringUtils.isNullOrWhitespaceOnly(propertyStr), "Missing application parameter '" + propertyName + "'");
        try {
            return Double.parseDouble(properties.getProperty(propertyName));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid value '" + propertyStr + "'for application parameter '" + propertyName + "'", e);
        }
    }

    public static float parseMandatoryFloat(Properties properties, String propertyName) {
        String propertyStr = properties.getProperty(propertyName);
        Preconditions.checkArgument(!StringUtils.isNullOrWhitespaceOnly(propertyStr), "Missing application parameter '" + propertyName + "'");
        try {
            return Float.parseFloat(properties.getProperty(propertyName));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid value '" + propertyStr + "'for application parameter '" + propertyName + "'", e);
        }
    }

    public static int parseMandatoryInt(Properties properties, String propertyName) {
        String propertyStr = properties.getProperty(propertyName);
        Preconditions.checkArgument(!StringUtils.isNullOrWhitespaceOnly(propertyStr), "Missing application parameter '" + propertyName + "'");
        try {
            return Integer.parseInt(properties.getProperty(propertyName));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid value '" + propertyStr + "'for application parameter '" + propertyName + "'", e);
        }
    }

    public static long parseMandatoryLong(Properties properties, String propertyName) {
        String propertyStr = properties.getProperty(propertyName);
        Preconditions.checkArgument(!StringUtils.isNullOrWhitespaceOnly(propertyStr), "Missing application parameter '" + propertyName + "'");
        try {
            return Long.parseLong(properties.getProperty(propertyName));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid value '" + propertyStr + "'for application parameter '" + propertyName + "'", e);
        }
    }
}
