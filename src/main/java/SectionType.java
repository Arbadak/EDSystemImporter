public enum SectionType {
    SYSTEM("system"),
    BODIES("bodies"),
    STATIONS("stations");

    private String value;

   SectionType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
