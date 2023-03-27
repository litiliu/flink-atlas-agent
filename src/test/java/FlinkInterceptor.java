import java.util.Objects;
import java.util.function.Function;

public class FlinkInterceptor {

    private static final String COMMENT = "--";

    public static String[] splitStatement(String statement) {
        StringBuilder sb = new StringBuilder();
        if (!Objects.isNull(statement)) {
            String[] arrays = statement.split("\n");
            for (String str : arrays) {
                if (!str.trim().startsWith(COMMENT)) {
                    sb.append(str);
                }
            }
        }
        return sb.toString().split(Constants.SQL_SEPARATOR);
    }

    public static String prepareStatement(String statement) {
        statement = removeNote.apply(statement);
        return statement.trim();
    }

    public static Function<String, String> removeNote = statement -> {
        if (!Objects.isNull(statement) && !statement.isEmpty()) {
            statement = statement.replaceAll("\u00A0", " ").replaceAll("--([^'\r\n]{0,}('[^'\r\n]{0,}'){0,1}[^'\r\n]{0,}){0,}", "").replaceAll("[\r\n]+", "\r\n").trim();
        }
        return statement;
    };
}
