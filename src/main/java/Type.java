public enum Type {
    SYSTEM("system"),
    BODIES("bodies"),
    STATIONS("stations");

    private String val;

    Type(String val) {
        this.val = val;
    }

    public String getVal() {
        return val;
    }
}
