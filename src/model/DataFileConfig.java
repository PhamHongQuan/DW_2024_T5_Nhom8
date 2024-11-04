package model;

public class DataFileConfig {
    private long id;
    private String sourcePath;
    private String location;
    private String format;
    private String columns;
    private String destination;

    // Constructors
    public DataFileConfig() {
    }

    public DataFileConfig(long id, String sourcePath, String location, String format, String columns, String destination) {
        this.id = id;
        this.sourcePath = sourcePath;
        this.location = location;
        this.format = format;
        this.columns = columns;
        this.destination = destination;
    }

    // Getters and Setters
    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getSourcePath() {
        return sourcePath;
    }

    public void setSourcePath(String sourcePath) {
        this.sourcePath = sourcePath;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public String getColumns() {
        return columns;
    }

    public void setColumns(String columns) {
        this.columns = columns;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    // toString method (optional, for debugging or logging)
    @Override
    public String toString() {
        return "model.DataFileConfig{" +
                "id=" + id +
                ", sourcePath='" + sourcePath + '\'' +
                ", location='" + location + '\'' +
                ", format='" + format + '\'' +
                ", columns='" + columns + '\'' +
                ", destination='" + destination + '\'' +
                '}';
    }
}
