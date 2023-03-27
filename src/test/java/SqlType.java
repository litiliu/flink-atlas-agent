
public enum SqlType {
    SELECT("SELECT"),
    CREATE("CREATE"),
    DROP("DROP"),
    ALTER("ALTER"),
    INSERT("INSERT"),
    DESC("DESC"),
    DESCRIBE("DESCRIBE"),
    EXPLAIN("EXPLAIN"),
    USE("USE"),
    SHOW("SHOW"),
    LOAD("LOAD"),
    UNLOAD("UNLOAD"),
    SET("SET"),
    RESET("RESET"),
    EXECUTE("EXECUTE"),
    UNKNOWN("UNKNOWN"),
    ;

    private String type;

    SqlType(String type) {
        this.type = type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public boolean equalsValue(String value) {
        return type.equalsIgnoreCase(value);
    }

    public boolean isInsert() {
        if ("INSERT".equalsIgnoreCase(type)) {
            return true;
        }
        return false;
    }

}
