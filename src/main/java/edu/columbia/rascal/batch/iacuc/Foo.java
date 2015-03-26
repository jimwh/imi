package edu.columbia.rascal.batch.iacuc;

import edu.columbia.rascal.business.service.Migrator;
import edu.columbia.rascal.business.service.review.iacuc.IacucStatus;
import edu.columbia.rascal.business.service.review.iacuc.IacucTaskForm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

@Component
public class Foo {

    // test tables
    private static final String SQL_TABLE_MIGRATOR = "select count(1) from ALL_TABLES where TABLE_NAME='IACUC_MIGRATOR'";
    private static final String SQL_TABLE_IMI = "select count(1) from ALL_TABLES where TABLE_NAME='IACUC_IMI'";
    private static final String SQL_TABLE_CORR = "select count(1) from ALL_TABLES where TABLE_NAME='IACUC_CORR'";
    private static final String SQL_TABLE_NOTE = "select count(1) from ALL_TABLES where TABLE_NAME='IACUC_NOTE'";

    // create tables
    private static final String SQL_CREATE_MIGRATOR = "CREATE table IACUC_MIGRATOR (TASKID_ NVARCHAR2(64) NOT NULL," +
            "STATUSID_ NVARCHAR2(10) NOT NULL, DATE_ TIMESTAMP(6) NOT NULL)";

    private static final String SQL_CREATE_CORR = "CREATE table IACUC_CORR(TASKID_ NVARCHAR2(64) NOT NULL," +
            "STATUSID_ NVARCHAR2(10) NOT NULL, DATE_ TIMESTAMP(6) NOT NULL)";

    private static final String SQL_CREATE_NOTE = "CREATE table IACUC_NOTE(TASKID_ NVARCHAR2(64) NOT NULL," +
            "STATUSID_ NVARCHAR2(10) NOT NULL, DATE_ TIMESTAMP(6) NOT NULL)";

    private static final String SQL_CREATE_IMI = "CREATE table IACUC_IMI(POID_ NVARCHAR2(10) NOT NULL, STATUSID_ NVARCHAR2(10) NOT NULL)";

    // drop tables
    private static final String SQL_DROP_MIGRATOR = "drop table IACUC_MIGRATOR purge";
    private static final String SQL_DROP_IMI = "drop table IACUC_IMI purge";
    private static final String SQL_DROP_NOTE = "drop table IACUC_NOTE purge";
    private static final String SQL_DROP_CORR = "drop table IACUC_CORR purge";

    // update table ACT_HI_TASKINST
    private static final String SQL_UPDATE_MIGRATOR = "update ACT_HI_TASKINST a" +
            " set (a.START_TIME_, a.END_TIME_, a.CLAIM_TIME_)=" +
            " (select m.DATE_, m.DATE_, m.DATE_ from IACUC_MIGRATOR m where a.ID_=m.TASKID_)" +
            " where exists ( select 1 from IACUC_MIGRATOR m where a.ID_=m.TASKID_)";

    private static final String SQL_UPDATE_CORR = "update ACT_HI_TASKINST a" +
            " set (a.START_TIME_, a.END_TIME_, a.CLAIM_TIME_)=" +
            " (select m.DATE_, m.DATE_, m.DATE_ from IACUC_CORR m where a.ID_=m.TASKID_)" +
            " where exists ( select 1 from IACUC_CORR m where a.ID_=m.TASKID_)";

    private static final String SQL_UPDATE_NOTE = "update ACT_HI_TASKINST a" +
            " set (a.START_TIME_, a.END_TIME_, a.CLAIM_TIME_)=" +
            " (select m.DATE_, m.DATE_, m.DATE_ from IACUC_NOTE m where a.ID_=m.TASKID_)" +
            " where exists ( select 1 from IACUC_NOTE m where a.ID_=m.TASKID_)";

    // select statement
    private static final String SQL_OLD_STATUS = "select OID, STATUSCODE, STATUSCODEDATE, USER_ID, IACUCPROTOCOLHEADERPER_OID, STATUSNOTES, NOTIFICATIONOID, ID" +
            " from IacucProtocolStatus s, RASCAL_USER u, IACUCPROTOCOLSNAPSHOT n" +
            " where s.STATUSSETBY=u.RID" +
            " and s.IACUCPROTOCOLHEADERPER_OID=?" +
            " and s.STATUSCODE not in ('Create', 'Release', 'UnRelease', 'Notify', 'Reject', 'PreApprove', " +
            " 'ChgApprovalDate','ChgEffectivDate','ChgEndDate','ChgMeetingDate'," +
            " 'HazardsApprove'," +
            " 'VetPreApproveB', 'VetPreApproveC','VetPreApproveE','VetPreApproveF','VetHoldB','VetHoldC','VetHoldE','VetHoldF') " +
            " and s.OID=n.IACUCPROTOCOLSTATUSID(+)" +
            " and s.OID not in (select STATUSID_ from IACUC_MIGRATOR where s.OID=to_number(STATUSID_) )" +
            " order by STATUSCODEDATE";

    private static final String SQL_KAPUT_STATUS_1 = "select OID, STATUSCODE, STATUSCODEDATE, USER_ID, IACUCPROTOCOLHEADERPER_OID, STATUSNOTES, NOTIFICATIONOID, ID" +
            " from IacucProtocolStatus s, RASCAL_USER u, IACUCPROTOCOLSNAPSHOT n" +
            " where s.STATUSSETBY=u.RID" +
            " and s.IACUCPROTOCOLHEADERPER_OID=?" +
            " and s.STATUSCODE<>'Create'" +
            " and s.OID=n.IACUCPROTOCOLSTATUSID(+)" +
            " and s.OID not in (select STATUSID_ from IACUC_MIGRATOR where s.OID=to_number(STATUSID_))" +
            " order by STATUSCODEDATE";

    private static final String SQL_KAPUT_STATUS_2 = "select OID, STATUSCODE, STATUSCODEDATE, USER_ID, IACUCPROTOCOLHEADERPER_OID, STATUSNOTES, NOTIFICATIONOID, ID" +
            " from IacucProtocolStatus s, RASCAL_USER u, IACUCPROTOCOLSNAPSHOT n" +
            " where s.STATUSSETBY=u.RID" +
            " and s.IACUCPROTOCOLHEADERPER_OID=?" +
            " and s.STATUSCODE not in ('Create', 'Submit', 'Distribute', 'ReturnToPI', 'Approve', 'Done'," +
            " 'Suspend','Terminate','Withdraw','Reinstate', 'ACCMemberHold', 'ACCMemberApprov') " +
            " and s.OID=n.IACUCPROTOCOLSTATUSID(+)" +
            " and s.OID not in (select STATUSID_ from IACUC_MIGRATOR where s.OID=to_number(STATUSID_))" +
            " order by STATUSCODEDATE";

    private static final String SQL_PROTOCOL_ID = "select distinct IACUCPROTOCOLHEADERPER_OID" +
            " from IACUCPROTOCOLSTATUS where STATUSCODE <> 'Create' order by IACUCPROTOCOLHEADERPER_OID";

    private static final String SQL_POID = "select distinct IACUCPROTOCOLHEADERPER_OID" +
            " from IACUCPROTOCOLSTATUS s" +
            " where EXISTS (select 1 from IACUC_IMI imi where s.OID= to_number(imi.statusid_) )" +
            " order by IACUCPROTOCOLHEADERPER_OID";

    private static final String SQL_ALLCORR = "select OID, IACUCPROTOCOLHEADERPER_OID protocolId, USER_ID, CREATIONDATE, RECIPIENTS, CARBONCOPIES, SUBJECT, CORRESPONDENCETEXT" +
            " from IacucCorrespondence c, RASCAL_USER u where c.AUTHORRID=u.RID" +
            " and OID not in (select STATUSID_ from IACUC_CORR) order by OID";


    private static final String SQL_OLD_NOTE="select OID, N.IACUCPROTOCOLHEADERPER_OID, U.USER_ID, N.NOTETEXT, N.LASTMODIFICATIONDATE" +
            " from IACUCPROTOCOLNOTES N, RASCAL_USER U" +
            " where N.NOTEAUTHOR is not null and U.RID=N.NOTEAUTHOR" +
            " order by N.IACUCPROTOCOLHEADERPER_OID";


    private static final Logger log = LoggerFactory.getLogger(Foo.class);

    private static final Set<String> EndSet = new HashSet<String>();
    static {
        EndSet.add("Withdraw");
        EndSet.add("ReturnToPI");
        EndSet.add("Approve");
        EndSet.add("Done");
        EndSet.add("Suspend");
        EndSet.add("Terminate");
        EndSet.add("Reinstate");
        EndSet.add("Notify");
        EndSet.add("ChgApprovalDate");
        EndSet.add("ChgEffectivDate");
        EndSet.add("ChgEndDate");
        EndSet.add("ChgMeetingDate");
        EndSet.add("HazardsApprove");
        EndSet.add("PreApprove");
        EndSet.add("Reject");
        EndSet.add("Release");
        EndSet.add("UnRelease");
        EndSet.add("VetHoldB");
        EndSet.add("VetHoldC");
        EndSet.add("VetHoldE");
        EndSet.add("VetHoldF");
        EndSet.add("VetPreApproveB");
        EndSet.add("VetPreApproveC");
        EndSet.add("VetPreApproveE");
        EndSet.add("VetPreApproveF");
    }


    private final JdbcTemplate jdbcTemplate;
    @Resource
    private Migrator migrator;

    @Autowired
    public Foo(JdbcTemplate jt) {
        this.jdbcTemplate = jt;
    }

    public void test() {
        //List<AttachedAppendix> list = migrator.getAttachedAppendix("5050");
        List<AttachedAppendix> list = migrator.getAttachedAppendix("0000");
        log.info("get appendix info...");
        for (AttachedAppendix a : list) {
            log.info("appendixType={},approveType={}, approvalDate={}", a.appendixType, a.approvalType, a.approvalDate);
        }
        log.info("done...");
    }

    public void testGetNote() {
        log.info("set up tables ...");
        if ( !hasNoteTable() ) {
            log.info("creating note table ...");
            createNoteTable();
        }
        List<OldNote> list = getAllOldNotes();
        for(OldNote note: list) {
            log.info("author={}, note={}, date={}", note.author, note.note, note.date);
        }
        log.info("import note...");
        importOldNote();
        jdbcTemplate.execute(SQL_UPDATE_NOTE);
    }

    public void testSubset() {
        log.info("test subset of data ...");
        migrator.abortProcess("95808", "testing");
        migrator.abortProcess("95800", "testing");
        migrator.abortProcess("92300", "testing");
        migrator.abortProcess("95657", "testing");
        migrator.abortProcess("95150", "testing");
        //
        setupTables();
        //
        List<String> plist = new ArrayList<String>();
        plist.add("90909");
        plist.add("92300");
        plist.add("95150");
        plist.add("95657");
        plist.add("95800");
        plist.add("95808");
        plist.add("96205");
        log.info("testing plist={}", plist.toString());
        walkThrough(plist);
        //
        updateMigrationTables();
        // printHistoryByBizKey(protocolId);
        List<CorrRcd> corrList = getAllCorr();
        log.info("corrSize={}", corrList.size());
    }

    public void startup() {
        log.info("set up tables ...");
        setupTables();
        // updateMigrationTables();
        //
        List<String> listProtocolId = getListProtocolId(SQL_PROTOCOL_ID);
        log.info("number of protocols: {}", listProtocolId.size());
        walkThrough(listProtocolId);
        //
        log.info("import corr...");
        importCorr();
        //
        log.info("import note...");
        importOldNote();
        //
        log.info("update migration tables...");
        updateMigrationTables();
    }

    private void importCorr() {
        List<CorrRcd> corrList = getAllCorr();
        log.info("corrList.size=" + corrList.size());
        for (CorrRcd corr : corrList) {
            migrator.importCorrRcd(corr);
        }
    }

    private void importOldNote() {
        List<OldNote> list = getAllOldNotes();
        log.info("number of old notes: {}", list.size());
        for (OldNote note : list) {
            migrator.importOldNote(note);
        }
    }

    // It may be mixed with finished or unfinished records.
    // Remove unfinished records and insert them into IMI table.
    // Import finished records as kaput status
    private void walkThrough(List<String> listProtocolId) {
        for (String protocolId : listProtocolId) {
            List<OldStatus> list = getOldStatusByProtocolId(protocolId, SQL_KAPUT_STATUS_1);
            // log.info("list.size={}", list.size());
            if (list == null || list.isEmpty()) continue;
            // last element is in the EndSet
            // which means these status had been done already
            int lastIndex = list.size() - 1;
            OldStatus lastStatus=list.get(lastIndex);
            if ( EndSet.contains(lastStatus.statusCode) ) {
                // log.info("lastIndex={}, lastStatus={}", lastIndex, lastStatus);
                migrator.importKaput(list);
                continue;
            }

            log.info("walk through...");
            // move from bottom up to Submit status
            Deque<OldStatus> deque=new LinkedList<OldStatus>();

            while (true) {
                int index = list.size() - 1;
                if (index < 0) break;
                if ("Submit".equals(list.get(index).statusCode)) {
                    deque.addFirst( list.get(index) );
                    OldStatus rcd = list.remove(index);
                    // save protocolId and statusId for next round
                    // migrator.insertToImiTable(rcd.protocolId, rcd.statusId);
                    break;
                } else {
                    deque.addFirst( list.get(index) );
                    list.remove( index );
                }
            }
            if ( !list.isEmpty() ) {
                log.info("left over list.size={}", list.size());
                migrator.importKaput(list);
            }

            if ( !deque.isEmpty() ) {
                log.info("deque.size={}", deque.size());
                migrator.migrateReviewInProgress(deque);
            }
        }
    }

    private void updateMigrationTables() {
        try {
            jdbcTemplate.execute(SQL_UPDATE_MIGRATOR);
            jdbcTemplate.execute(SQL_UPDATE_CORR);
            jdbcTemplate.execute(SQL_UPDATE_NOTE);
        } catch (Exception e) {
            log.error("err in update table:", e);
        }
    }

    private List<OldStatus> getOldStatusByProtocolId(String protocolId, String sql) {

        RowMapper<OldStatus> mapper = new RowMapper<OldStatus>() {
            @Override
            public OldStatus mapRow(ResultSet rs, int rowNum) throws SQLException {
                OldStatus rcd = new OldStatus(
                        rs.getString("OID"),
                        rs.getString("STATUSCODE"),
                        rs.getTimestamp("STATUSCODEDATE"),
                        rs.getString("USER_ID"),
                        rs.getString("IACUCPROTOCOLHEADERPER_OID"),
                        rs.getString("STATUSNOTES"),
                        rs.getString("NOTIFICATIONOID"),
                        rs.getString("ID")
                );
                return rcd;
            }
        };
        return this.jdbcTemplate.query(sql, mapper, protocolId);
    }

    public List<String> getListProtocolId(String sql) {
        RowMapper<String> mapper = new RowMapper<String>() {
            @Override
            public String mapRow(ResultSet rs, int rowNum) throws SQLException {
                return rs.getString("IACUCPROTOCOLHEADERPER_OID");
            }
        };
        return this.jdbcTemplate.query(sql, mapper);
    }

    private boolean hasMigratorTable() {
        int one = jdbcTemplate.queryForObject(SQL_TABLE_MIGRATOR, Integer.class);
        return one == 1;
    }

    private boolean hasCorrTable() {
        int one = jdbcTemplate.queryForObject(SQL_TABLE_CORR, Integer.class);
        return one == 1;
    }

    private boolean hasNoteTable() {
        int one = jdbcTemplate.queryForObject(SQL_TABLE_NOTE, Integer.class);
        return one == 1;
    }

    private boolean hasImiTable() {
        int one = jdbcTemplate.queryForObject(SQL_TABLE_IMI, Integer.class);
        return one == 1;
    }

    private void createMigratorTable() {
        jdbcTemplate.execute(SQL_CREATE_MIGRATOR);
    }

    private void createCorrTable() {
        jdbcTemplate.execute(SQL_CREATE_CORR);
    }

    private void createImiTable() {
        jdbcTemplate.execute(SQL_CREATE_IMI);
    }

    private void createNoteTable() {
        jdbcTemplate.execute(SQL_CREATE_NOTE);
    }

    private void dropMigratorTable() {
        jdbcTemplate.execute(SQL_DROP_MIGRATOR);
    }
    private void dropCorrTable() {
        jdbcTemplate.execute(SQL_DROP_CORR);
    }
    private void dropNoteTable() {
        jdbcTemplate.execute(SQL_DROP_NOTE);
    }

    public List<CorrRcd> getAllCorr() {
        RowMapper<CorrRcd> mapper = new RowMapper<CorrRcd>() {
            @Override
            public CorrRcd mapRow(ResultSet rs, int rowNum) throws SQLException {
                CorrRcd rcd = new CorrRcd(
                        rs.getString("OID"),
                        rs.getString("protocolId"),
                        rs.getString("USER_ID"),
                        rs.getTimestamp("CREATIONDATE"),
                        rs.getString("RECIPIENTS"),
                        rs.getString("CARBONCOPIES"),
                        rs.getString("SUBJECT"),
                        rs.getString("CORRESPONDENCETEXT")
                );
                return rcd;
            }
        };
        return this.jdbcTemplate.query(SQL_ALLCORR, mapper);
    }

    public List<OldNote> getAllOldNotes() {
        RowMapper<OldNote> mapper = new RowMapper<OldNote>() {
            @Override
            public OldNote mapRow(ResultSet rs, int rowNum) throws SQLException {
                OldNote rcd = new OldNote(
                        rs.getString("OID"),
                        rs.getString("IACUCPROTOCOLHEADERPER_OID"),
                        rs.getString("USER_ID"),
                        rs.getString("NOTETEXT"),
                        rs.getTimestamp("LASTMODIFICATIONDATE")
                );
                return rcd;
            }
        };
        return this.jdbcTemplate.query(SQL_OLD_NOTE, mapper);
    }

    private void setupTables() {
        if (!hasMigratorTable()) {
            log.info("creating migration table ...");
            createMigratorTable();
        }
        if (!hasCorrTable()) {
            log.info("creating corr table ...");
            createCorrTable();
        }
        if (!hasImiTable()) {
            log.info("creating imi table ...");
            createImiTable();
        }
        if ( !hasNoteTable() ) {
            log.info("creating note table ...");
            createNoteTable();
        }

    }

    public void printHistoryByBizKey(String protocolId) {
        List<IacucTaskForm> list=migrator.getIacucProtocolHistory(protocolId);
        for(IacucTaskForm form: list) {
            log.info("taskDefKey={}, taskName={}, author={}, endTime={}",
                    form.getTaskDefKey(), form.getTaskName(), form.getAuthor(),form.getEndTimeString() );
        }
    }
}
