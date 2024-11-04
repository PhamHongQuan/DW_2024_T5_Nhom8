package model;

public class DataFile {
    private long id;
    private int dfConfigId;
    private String name;
    private int rowCount;
    private String status;
    private String note;

    public DataFile() {
    }

    public DataFile(long id, int dfConfigId, String name, int rowCount, String status, String note) {
        this.id = id;
        this.dfConfigId = dfConfigId;
        this.name = name;
        this.rowCount = rowCount;
        this.status = status;
        this.note = note;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public int getDfConfigId() {
        return dfConfigId;
    }

    public void setDfConfigId(int dfConfigId) {
        this.dfConfigId = dfConfigId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getRowCount() {
        return rowCount;
    }

    public void setRowCount(int rowCount) {
        this.rowCount = rowCount;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getNote() {
        return note;
    }

    public void setNote(String note) {
        this.note = note;
    }

    @Override
    public String toString() {
        return "DataFile{" +
                "id=" + id +
                ", dfConfigId='" + dfConfigId + '\'' +
                ", name='" + name + '\'' +
                ", rowCount=" + rowCount +
                ", status='" + status + '\'' +
                ", note='" + note + '\'' +
                '}';
    }
}
