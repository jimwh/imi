package edu.columbia.rascal.batch.iacuc;

import java.util.Date;

public class OldNote {

    public final String oid;
    public final String protocolId;
    public final String author;
    public final String note;
    public final Date date;

    public OldNote(String oid, String protocolId, String author, String note, Date date) {
        this.oid=oid;
        this.protocolId=protocolId;
        this.author=author;
        this.note=note;
        this.date=date;
    }

}
