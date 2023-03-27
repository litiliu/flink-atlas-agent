public class Executor {

    public String pretreatStatement(String statement) {
        return FlinkInterceptor.prepareStatement(statement);
    }
}
