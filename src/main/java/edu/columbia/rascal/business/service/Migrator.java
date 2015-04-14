package edu.columbia.rascal.business.service;

import edu.columbia.rascal.batch.iacuc.*;

import edu.columbia.rascal.business.service.review.iacuc.*;
import org.activiti.engine.ManagementService;
import org.activiti.engine.impl.cmd.AbstractCustomSqlExecution;
import org.activiti.engine.impl.cmd.CustomSqlExecution;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.support.AbstractLobStreamingResultSetExtractor;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;


@Service
public class Migrator {

    private static final Logger log = LoggerFactory.getLogger(Migrator.class);

    private static final String SQL_INSERT_MIGRATOR = "insert into IACUC_MIGRATOR (TASKID_, STATUSID_, DATE_) VALUES (?, ?, ?)";
    private static final String SQL_INSERT_ADVERSE_MIGRATOR = "insert into IACUC_ADVERSE_MIGRATOR (TASKID_, STATUSID_, DATE_) VALUES (?, ?, ?)";

    private static final String SQL_INSERT_CORR = "insert into IACUC_CORR (TASKID_, STATUSID_, DATE_) VALUES (?, ?, ?)";
    private static final String SQL_INSERT_ATTACHED_CORR = "insert into IACUC_ATTACHED_CORR (STATUSID_, CORRID_, DATE_) VALUES (?, ?, ?)";
    private static final String SQL_INSERT_ADVERSE_ATTACHED_CORR = "insert into IACUC_ADVERSE_ATTACHED_CORR (STATUSID_, CORRID_, DATE_) VALUES (?, ?, ?)";

    private static final String SQL_INSERT_NOTE = "insert into IACUC_NOTE (TASKID_, STATUSID_, DATE_) VALUES (?, ?, ?)";

    private static final String SQL_INSERT_IMI = "insert into IACUC_IMI (POID_, STATUSID_) VALUES (?, ?)";
    /*
    private static final String SQL_INSERT_IMI =
            "insert into IACUC_IMI (POID_, STATUSID_)" +
            " select :POID_, :STATUSID_ from dual" +
            " where not exists (select 1 from IACUC_IMI where POID_=:POID_)";

    private static final String SQL_INSERT_IMI =
    "MERGE INTO IACUC_IMI a USING DUAL ON (a.POID_= :1)"+
    " WHEN NOT MATCHED THEN INSERT(POID_, STATUSID_) VALUES (:2, :3)";
    */

    private static final String SQL_SNAPSHOT = "select CREATIONDATE, FILECONTEXT from IACUCPROTOCOLSNAPSHOT where ID=?";
    private static final String SQL_ADVERSE_SNAPSHOT = "select CREATIONDATE, FILECONTEXT from IACUCADVERSEEVENTSNAPSHOT where ID=?";
    //
    private static final String SQL_CORR = "select OID, IACUCPROTOCOLHEADERPER_OID protocolId,  USER_ID, CREATIONDATE, RECIPIENTS, CARBONCOPIES, SUBJECT, CORRESPONDENCETEXT" +
            " from IacucCorrespondence c, RASCAL_USER u where c.AUTHORRID=u.RID and c.OID=?";
    //
    private static final String SQL_ADVERSE_CORR=
    "select OID, c.IACUCADVERSEEVENT_OID,  USER_ID, CREATIONDATE, RECIPIENTS, CARBONCOPIES, SUBJECT, CORRESPONDENCETEXT"+
    " from IACUCADVERSEEVENTCORRESPOND c, RASCAL_USER u where c.AUTHORRID=u.RID and c.OID=?";

    //
    private static final String SQL_RV = "select OID,IACUCPROTOCOLSTATUSPER_OID STATUSID,REVIEWTYPE," +
            "(select user_id from Rascal_User where RID=REVIEWER1) A," +
            "(select user_id from Rascal_User where RID=REVIEWER2) B," +
            "(select user_id from Rascal_User where RID=REVIEWER3) C," +
            "MEETINGDATE from IacucProtocolReview where IACUCPROTOCOLSTATUSPER_OID=?";
    //
    private static final String SQL_APPENDIX = "select APPENDIXTYPE, APPROVALTYPE, APPROVALDATE " +
            "from APPENDIXAPPROVAL a, APPENDIXTRACKING t " +
            "where a.FK_TRACKING_ID=t.OID and t.OWNERTYPE='IacucProtocolHeader'" +
            "and t.OWNEROID=?";


    /**
     * kaput the status should cover ALL names in the view history page
     */
    private static final Map<String, String> CodeToName = new HashMap<String, String>();
    static {
        CodeToName.put("ACCMemberApprov", IacucStatus.Rv1Approval.statusName());
        CodeToName.put("ACCMemberHold", IacucStatus.Rv1Hold.statusName());
        CodeToName.put("FullReviewReq", IacucStatus.Rv1ReqFullReview.statusName());
        CodeToName.put("SOPreApproveA", IacucStatus.SOPreApproveA.statusName());
        CodeToName.put("SOPreApproveB", IacucStatus.SOPreApproveB.statusName());
        CodeToName.put("SOPreApproveC", IacucStatus.SOPreApproveC.statusName());
        CodeToName.put("SOPreApproveD", IacucStatus.SOPreApproveD.statusName());
        CodeToName.put("SOPreApproveE", IacucStatus.SOPreApproveE.statusName());
        CodeToName.put("SOPreApproveF", IacucStatus.SOPreApproveF.statusName());
        CodeToName.put("SOPreApproveG", IacucStatus.SOPreApproveG.statusName());
        CodeToName.put("SOPreApproveI", IacucStatus.SOPreApproveI.statusName());
        CodeToName.put("SOHoldA", IacucStatus.SOHoldA.statusName());
        CodeToName.put("SOHoldB", IacucStatus.SOHoldB.statusName());
        CodeToName.put("SOHoldC", IacucStatus.SOHoldC.statusName());
        CodeToName.put("SOHoldD", IacucStatus.SOHoldD.statusName());
        CodeToName.put("SOHoldE", IacucStatus.SOHoldE.statusName());
        CodeToName.put("SOHoldF", IacucStatus.SOHoldF.statusName());
        CodeToName.put("SOHoldG", IacucStatus.SOHoldG.statusName());
        CodeToName.put("SOHoldI", IacucStatus.SOHoldI.statusName());
        CodeToName.put("ReturnToPI", IacucStatus.ReturnToPI.statusName());
        CodeToName.put("Terminate", IacucStatus.Terminate.statusName());
        CodeToName.put("Suspend", IacucStatus.Suspend.statusName());
        CodeToName.put("Withdraw", IacucStatus.Withdraw.statusName());
        CodeToName.put("Approve", IacucStatus.FinalApproval.statusName());
    }


    private final JdbcTemplate jdbcTemplate;

    @Resource
    private ManagementService managementService;
    @Resource
    private IacucProcessService processService;

    @Autowired
    public Migrator(JdbcTemplate jt) {
        this.jdbcTemplate = jt;
    }

    public void importKaput(List<OldStatus> kaputList) {
        Map<String, Object>processInput=new HashMap<String, Object>();
        int kaputCount=kaputList.size();
        // log.info("kaputCount={}", kaputCount);
        processInput.put("START_GATEWAY", IacucStatus.Kaput.gatewayValue());
        processInput.put("kaputCount", kaputCount);
        OldStatus startUpStatus=kaputList.get(0);
        String processInstanceId = processService.startKaputProcess(startUpStatus.protocolId, startUpStatus.userId, processInput);
        if (processInstanceId != null) {
            insertToMigratorTable(processInstanceId, startUpStatus.statusId, startUpStatus.statusCodeDate);
        } else {
            log.error("failed to import kaput: {}", startUpStatus);
            return;
        }
        for (OldStatus status : kaputList) {
            // log.info("kaput: {}", status);
            completeKaputTask(status);
        }
    }

    public void importKaputAdverse(List<OldAdverseStatus> kaputList) {
        Map<String, Object>processInput=new HashMap<String, Object>();
        int kaputCount=kaputList.size();
        log.info("kaputCount={}", kaputCount);
        processInput.put("START_GATEWAY", IacucStatus.Kaput.gatewayValue());
        processInput.put("kaputCount", kaputCount);
        OldAdverseStatus startUpStatus=kaputList.get(0);
        String processInstanceId = processService.startAdverseKaputProcess(startUpStatus.adverseId, startUpStatus.userId, processInput);
        if (processInstanceId != null) {
            insertToMigratorTable(processInstanceId, startUpStatus.adverseStatusId, startUpStatus.statusCodeDate);
        } else {
            log.error("failed to import kaput: {}", startUpStatus);
            return;
        }
        for (OldAdverseStatus status : kaputList) {
            log.info("kaput: {}", status);
            completeKaputAdverseTask(status);
        }
    }

    private void insertToMigratorTable(String taskId, String statusId, Date date) {
        this.jdbcTemplate.update(SQL_INSERT_MIGRATOR, taskId, statusId, date);
    }

    private void insertToAdverseMigratorTable(String taskId, String statusId, Date date) {
        this.jdbcTemplate.update(SQL_INSERT_ADVERSE_MIGRATOR, taskId, statusId, date);
    }

    private void insertToCorrTable(String taskId, String corrOid, Date date) {
        this.jdbcTemplate.update(SQL_INSERT_CORR, taskId, corrOid, date);
    }

    public void insertToAttachedCorrTable(String statusId, String corrOid, Date date) {
        this.jdbcTemplate.update(SQL_INSERT_ATTACHED_CORR, statusId, corrOid, date);
    }

    public void insertToAdverseAttachedCorrTable(String statusId, String corrOid, Date date) {
        this.jdbcTemplate.update(SQL_INSERT_ADVERSE_ATTACHED_CORR, statusId, corrOid, date);
    }

    private void insertToNoteTable(String taskId, String noteOid, Date date) {
        this.jdbcTemplate.update(SQL_INSERT_NOTE, taskId, noteOid, date);
    }

    public void insertToImiTable(String protocolId, String statusId) {
        this.jdbcTemplate.update(SQL_INSERT_IMI, protocolId, statusId);
    }

    public void migrateReviewInProgress(Deque<OldStatus> list) {
        for (OldStatus status : list) {
            migrateReviewInProgress(status);
        }
    }

    private void migrateReviewInProgress(OldStatus status) {

        if (IacucStatus.Submit.isStatus(status.statusCode)) {
            submitProtocol(status);
        } else if ("Distribute".equals(status.statusCode)) {
            // ugly case distribution for approval
            distributeProtocol(status);
        } else if ("ACCMemberHold".equals(status.statusCode)) {
            if (!completeAssigneeHoldReview(status)) {
                log.error("err in completeAssigneeHoldReview: " + status);
            }
        } else if ("ACCMemberApprov".equals(status.statusCode)) {
            if (!completeAssigneeApprovalReview(status)) {
                log.error("err in completeAssigneeApprovalReview: " + status);
            }
        } else if ("FullReviewReq".equals(status.statusCode)) {
            if (!completeAssigneeFullReq(status)) {
                log.error("err in completeAssigneeFullReq: " + status);
            }
        } else if (IacucStatus.SOPreApproveA.isStatus(status.statusCode)) {
            completeAppendixTask(IacucStatus.SOPreApproveA.taskDefKey(), IacucStatus.SOPreApproveA.statusName(), status);
        } else if (IacucStatus.SOPreApproveB.isStatus(status.statusCode)) {
            completeAppendixTask(IacucStatus.SOPreApproveB.taskDefKey(), IacucStatus.SOPreApproveB.statusName(), status);

        } else if (IacucStatus.SOPreApproveC.isStatus(status.statusCode)) {
            completeAppendixTask(IacucStatus.SOPreApproveC.taskDefKey(), IacucStatus.SOPreApproveC.statusName(), status);

        } else if (IacucStatus.SOPreApproveD.isStatus(status.statusCode)) {
            completeAppendixTask(IacucStatus.SOPreApproveD.taskDefKey(), IacucStatus.SOPreApproveD.statusName(), status);

        } else if (IacucStatus.SOPreApproveE.isStatus(status.statusCode)) {
            completeAppendixTask(IacucStatus.SOPreApproveE.taskDefKey(), IacucStatus.SOPreApproveE.statusName(), status);

        } else if (IacucStatus.SOPreApproveF.isStatus(status.statusCode)) {
            completeAppendixTask(IacucStatus.SOPreApproveF.taskDefKey(), IacucStatus.SOPreApproveF.statusName(), status);

        } else if (IacucStatus.SOPreApproveG.isStatus(status.statusCode)) {
            completeAppendixTask(IacucStatus.SOPreApproveG.taskDefKey(), IacucStatus.SOPreApproveG.statusName(), status);

        } else if (IacucStatus.SOPreApproveI.isStatus(status.statusCode)) {
            completeAppendixTask(IacucStatus.SOPreApproveI.taskDefKey(), IacucStatus.SOPreApproveI.statusName(), status);

        } else if (IacucStatus.SOHoldA.isStatus(status.statusCode)) {
            completeAppendixTask(IacucStatus.SOHoldA.taskDefKey(), IacucStatus.SOHoldA.statusName(), status);
        } else if (IacucStatus.SOHoldB.isStatus(status.statusCode)) {
            completeAppendixTask(IacucStatus.SOHoldB.taskDefKey(), IacucStatus.SOHoldB.statusName(), status);
        } else if (IacucStatus.SOHoldC.isStatus(status.statusCode)) {
            completeAppendixTask(IacucStatus.SOHoldC.taskDefKey(), IacucStatus.SOHoldC.statusName(), status);
        } else if (IacucStatus.SOHoldD.isStatus(status.statusCode)) {
            completeAppendixTask(IacucStatus.SOHoldD.taskDefKey(), IacucStatus.SOHoldD.statusName(), status);
        } else if (IacucStatus.SOHoldE.isStatus(status.statusCode)) {
            completeAppendixTask(IacucStatus.SOHoldE.taskDefKey(), IacucStatus.SOHoldE.statusName(), status);
        } else if (IacucStatus.SOHoldF.isStatus(status.statusCode)) {
            completeAppendixTask(IacucStatus.SOHoldF.taskDefKey(), IacucStatus.SOHoldF.statusName(), status);
        } else if (IacucStatus.SOHoldG.isStatus(status.statusCode)) {
            completeAppendixTask(IacucStatus.SOHoldG.taskDefKey(), IacucStatus.SOHoldG.statusName(), status);
        } else if (IacucStatus.SOHoldI.isStatus(status.statusCode)) {
            completeAppendixTask(IacucStatus.SOHoldI.taskDefKey(), IacucStatus.SOHoldI.statusName(), status);
        } else {
            // log.error("treat it as kaput for unhandled status: {}",status);
            importKaputStatus(status);
        }

    }

    private boolean completeAppendixTask(String taskDefKey, String taskName, OldStatus status) {
        if (!hasTask(status.protocolId, taskDefKey)) {
            log.error("no appendix task for status={} ", status);
            return false;
        }

        IacucTaskForm form = new IacucTaskForm();
        form.setBizKey(status.protocolId);
        form.setAuthor(status.userId);
        form.setTaskDefKey(taskDefKey);
        form.setTaskName(taskName);
        form.setComment(status.statusNote);
        attachSnapshotToTask(taskDefKey, status, form);
        String taskId = processService.completeTaskByTaskForm(IacucProcessService.ProtocolProcessDefKey, form);
        if (taskId != null) {
            insertToMigratorTable(taskId, status.statusId, status.statusCodeDate);
            return true;
        }
        return false;
    }

    private boolean distributeProtocol(OldStatus status) {

        // first from status id get reviewer record
        ReviewRcd reviewRcd = getReviewRcdByStatusId(status.statusId);
        if (reviewRcd == null) {
            log.error("unable to find ReviewInfo by status=" + status);
            return false;
        }

        /* "Sub-Committee"
           don't use meetingDate for testing!!!
        */
        if( "Sub-Committee".equalsIgnoreCase(reviewRcd.reviewType) ) {
            if ( !hasTask(status.protocolId, IacucStatus.DistributeSubcommittee.taskDefKey()) ) {
                log.error("no subcommittee for protocolId={} ", status.protocolId);
                return false;
            }
            // go sub-committee
            IacucDistributeSubcommitteeForm form = new IacucDistributeSubcommitteeForm();
            form.setTaskDefKey(IacucStatus.DistributeSubcommittee.taskDefKey());
            form.setTaskName(IacucStatus.DistributeSubcommittee.statusName());
            form.setBizKey(status.protocolId);
            form.setAuthor(status.userId);
            if (!StringUtils.isBlank(status.snapshotId)) {
                attachSnapshotToTask(IacucStatus.DistributeSubcommittee.taskDefKey(), status, form);
            }
            form.setDate(reviewRcd.meetingDate);
            form.setComment(status.statusNote);
            String taskId = processService.completeTaskByTaskForm(IacucProcessService.ProtocolProcessDefKey, form);
            if (taskId != null) insertToMigratorTable(taskId, status.statusId, status.statusCodeDate);
            return true;
        } else {
            if (!hasTask(status.protocolId, IacucStatus.DistributeReviewer.taskDefKey())) {
                log.error("no distribute to reviewers for protocolId={} ",
                        status.protocolId);
                return false;
            }

            // go distribute to designated reviewers
            IacucDistributeReviewerForm form = new IacucDistributeReviewerForm();
            form.setTaskDefKey(IacucStatus.DistributeReviewer.taskDefKey());
            form.setTaskName(IacucStatus.DistributeReviewer.statusName());
            form.setBizKey(status.protocolId);
            form.setAuthor(status.userId);
            if (!StringUtils.isBlank(status.snapshotId)) {
                attachSnapshotToTask(IacucStatus.DistributeReviewer.taskDefKey(), status, form);
            }
            form.setComment(status.statusNote);
            List<String> reviewerList = new ArrayList<String>();
            addReviewers(reviewerList, reviewRcd.reviewer1);
            addReviewers(reviewerList, reviewRcd.reviewer2);
            addReviewers(reviewerList, reviewRcd.reviewer3);
            if (reviewerList.isEmpty()) {
                log.error("no reviewers for distribution: status={}, reviewerRcd={}", status, reviewRcd);
                return false;
            }
            form.setReviewerList(reviewerList);
            //
            CorrRcd corrRcd = getCorrByCorrId(status.notificationId);
            if (corrRcd != null) {
                IacucCorrespondence corr = new IacucCorrespondence();
                corr.setCreationDate(corrRcd.creationDate);
                corr.setFrom(corrRcd.fromUserId);
                corr.setRecipient(corrRcd.to);
                corr.setSubject(corrRcd.subject);
                corr.setText(corrRcd.body);
                form.setCorrespondence(corr);
            }

            String taskId = processService.completeTaskByTaskForm(IacucProcessService.ProtocolProcessDefKey, form);
            if( taskId==null ) return false;
            insertToMigratorTable(taskId, status.statusId, status.statusCodeDate);
            if (corrRcd != null) {
                insertToAttachedCorrTable(status.statusId, corrRcd.oid, corrRcd.creationDate);
            }
            return false;
        }
    }

    private CorrRcd getCorrByCorrId(String notificationId) {
        if (StringUtils.isBlank(notificationId)) {
            return null;
        }
        CorrRcd corrRcd = getCorrRcdByNotificationId(notificationId);
        if (corrRcd == null) {
            return null;
        }
        if (StringUtils.isBlank(corrRcd.body)) {
            return null;
        }
        return corrRcd;
    }

    private AdverseCorr getAdverseCorrByCorrId(String notificationId) {
        if (StringUtils.isBlank(notificationId)) {
            return null;
        }
        AdverseCorr corrRcd = getAdverseCorrRcdByNotificationId(notificationId);
        if (corrRcd == null) {
            return null;
        }
        if (StringUtils.isBlank(corrRcd.text)) {
            return null;
        }
        return corrRcd;
    }

    private void addReviewers(List<String> reviewerList, String reviewer) {
        if (!StringUtils.isBlank(reviewer)) {
            reviewerList.add(reviewer);
        }
    }

    private boolean completeAssigneeApprovalReview(OldStatus status) {
        if (!hasTaskForReviewer(status.protocolId)) {
            log.error("no reviewer task status={}", status);
            return false;
        }

        String reviewer = status.userId;
        if (StringUtils.isBlank(reviewer)) {
            log.error("no reviewer task status={}", status);
            return false;
        }

        IacucTaskForm form = new IacucTaskForm();
        form.setBizKey(status.protocolId);
        form.setAuthor(status.userId);
        form.setComment(status.statusNote);
        if (reviewer.equals(processService.getTaskAssignee(IacucStatus.Rv1Approval.taskDefKey(), status.protocolId))) {
            form.setTaskDefKey(IacucStatus.Rv1Approval.taskDefKey());
            form.setTaskName(IacucStatus.Rv1Approval.statusName());
            attachSnapshotToTask(IacucStatus.Rv1Approval.taskDefKey(), status, form);
            String taskId = processService.completeTaskByTaskForm(IacucProcessService.ProtocolProcessDefKey, form);
            if (taskId != null) insertToMigratorTable(taskId, status.statusId, status.statusCodeDate);

        } else if (reviewer.equals(processService.getTaskAssignee(IacucStatus.Rv2Approval.taskDefKey(), status.protocolId))) {
            attachSnapshotToTask(IacucStatus.Rv2Approval.taskDefKey(), status, form);
            form.setTaskDefKey(IacucStatus.Rv2Approval.taskDefKey());
            form.setTaskName(IacucStatus.Rv2Approval.statusName());
            String taskId = processService.completeTaskByTaskForm(IacucProcessService.ProtocolProcessDefKey, form);
            if (taskId != null) insertToMigratorTable(taskId, status.statusId, status.statusCodeDate);

        } else if (reviewer.equals(processService.getTaskAssignee(IacucStatus.Rv3Approval.taskDefKey(), status.protocolId))) {
            attachSnapshotToTask(IacucStatus.Rv3Approval.taskDefKey(), status, form);
            form.setTaskDefKey(IacucStatus.Rv3Approval.taskDefKey());
            form.setTaskName(IacucStatus.Rv3Approval.statusName());
            String taskId = processService.completeTaskByTaskForm(IacucProcessService.ProtocolProcessDefKey, form);
            if (taskId != null) insertToMigratorTable(taskId, status.statusId, status.statusCodeDate);

        } else {
            log.error("no reviewer task status={}", status);
            return false;
        }

        return true;
    }

    private boolean completeAssigneeHoldReview(OldStatus status) {
        if (!hasTaskForReviewer(status.protocolId)) {
            log.error("no reviewer task status={}", status);
            return false;
        }

        String reviewer = status.userId;
        if (StringUtils.isBlank(reviewer)) {
            log.error("no reviewer task status={}", status);
            return false;
        }

        IacucTaskForm form = new IacucTaskForm();
        form.setBizKey(status.protocolId);
        form.setAuthor(status.userId);
        form.setComment(status.statusNote);
        if (reviewer.equals(processService.getTaskAssignee(IacucStatus.Rv1Hold.taskDefKey(), status.protocolId))) {
            form.setTaskDefKey(IacucStatus.Rv1Hold.taskDefKey());
            form.setTaskName(IacucStatus.Rv1Hold.statusName());
            attachSnapshotToTask(IacucStatus.Rv1Hold.taskDefKey(), status, form);
            String taskId = processService.completeTaskByTaskForm(IacucProcessService.ProtocolProcessDefKey, form);
            if (taskId != null) insertToMigratorTable(taskId, status.statusId, status.statusCodeDate);

        } else if (reviewer.equals(processService.getTaskAssignee(IacucStatus.Rv2Hold.taskDefKey(), status.protocolId))) {
            attachSnapshotToTask(IacucStatus.Rv2Hold.taskDefKey(), status, form);
            form.setTaskDefKey(IacucStatus.Rv2Hold.taskDefKey());
            form.setTaskName(IacucStatus.Rv2Hold.statusName());
            String taskId = processService.completeTaskByTaskForm(IacucProcessService.ProtocolProcessDefKey, form);
            if (taskId != null) insertToMigratorTable(taskId, status.statusId, status.statusCodeDate);

        } else if (reviewer.equals(processService.getTaskAssignee(IacucStatus.Rv3Hold.taskDefKey(), status.protocolId))) {
            attachSnapshotToTask(IacucStatus.Rv3Hold.taskDefKey(), status, form);
            form.setTaskDefKey(IacucStatus.Rv3Hold.taskDefKey());
            form.setTaskName(IacucStatus.Rv3Hold.statusName());
            String taskId = processService.completeTaskByTaskForm(IacucProcessService.ProtocolProcessDefKey, form);
            if (taskId != null) insertToMigratorTable(taskId, status.statusId, status.statusCodeDate);

        } else {
            log.error("no reviewer task status={}", status);
            return false;
        }

        return true;

    }


    private boolean completeAssigneeFullReq(OldStatus status) {
        if (!hasTaskForReviewer(status.protocolId)) {
            log.error("no reviewer task status={}", status);
            return false;
        }

        String reviewer = status.userId;
        if (StringUtils.isBlank(reviewer)) {
            log.error("no reviewer task status={}", status);
            return false;
        }

        IacucTaskForm form = new IacucTaskForm();
        form.setBizKey(status.protocolId);
        form.setAuthor(status.userId);
        form.setComment(status.statusNote);
        if (reviewer.equals(processService.getTaskAssignee(IacucStatus.Rv1ReqFullReview.taskDefKey(), status.protocolId))) {
            form.setTaskDefKey(IacucStatus.Rv1ReqFullReview.taskDefKey());
            form.setTaskName(IacucStatus.Rv1ReqFullReview.statusName());
            attachSnapshotToTask(IacucStatus.Rv1ReqFullReview.taskDefKey(), status, form);
            String taskId = processService.completeTaskByTaskForm(IacucProcessService.ProtocolProcessDefKey, form);
            if (taskId != null) insertToMigratorTable(taskId, status.statusId, status.statusCodeDate);

        } else if (reviewer.equals(processService.getTaskAssignee(IacucStatus.Rv2ReqFullReview.taskDefKey(), status.protocolId))) {
            attachSnapshotToTask(IacucStatus.Rv2ReqFullReview.taskDefKey(), status, form);
            form.setTaskDefKey(IacucStatus.Rv2ReqFullReview.taskDefKey());
            form.setTaskName(IacucStatus.Rv2ReqFullReview.statusName());
            String taskId = processService.completeTaskByTaskForm(IacucProcessService.ProtocolProcessDefKey, form);
            if (taskId != null) insertToMigratorTable(taskId, status.statusId, status.statusCodeDate);

        } else if (reviewer.equals(processService.getTaskAssignee(IacucStatus.Rv3ReqFullReview.taskDefKey(), status.protocolId))) {
            attachSnapshotToTask(IacucStatus.Rv3ReqFullReview.taskDefKey(), status, form);
            form.setTaskDefKey(IacucStatus.Rv3ReqFullReview.taskDefKey());
            form.setTaskName(IacucStatus.Rv3ReqFullReview.statusName());
            String taskId = processService.completeTaskByTaskForm(IacucProcessService.ProtocolProcessDefKey, form);
            if (taskId != null) insertToMigratorTable(taskId, status.statusId, status.statusCodeDate);

        } else {
            log.error("no reviewer task status={}", status);
            return false;
        }

        return true;

    }

    private boolean submitProtocol(OldStatus status) {
        if (processService.isProtocolProcessStarted(status.protocolId)) {
            log.error("process was already started for status={}", status);
            return false;
        } else {
            Map<String, Object> processInputMap = getProcessInputMap(status);
            return startProcess(status, processInputMap) != null;
        }
    }

    private Map<String, Object> getProcessInputMap(OldStatus status) {
        Map<String, Object> map = new HashMap<String, Object>();
        List<AttachedAppendix> list = getAttachedAppendix(status.protocolId);
        if (list.isEmpty()) return map;
        for (AttachedAppendix a : list) {
            if (!"approve".equals(a.approvalType)) {
                map.put(IacucProcessService.GetAppendixMapKey(a.appendixType), true);
            }
        }
        return map;
    }

    private String startProcess(OldStatus status, Map<String, Object> processInputMap) {
        String processId = processService.startProtocolProcess(status.protocolId, status.userId, processInputMap);
        insertToMigratorTable(processId, status.statusId, status.statusCodeDate);

        IacucTaskForm taskForm = new IacucTaskForm();
        taskForm.setBizKey(status.protocolId);
        taskForm.setAuthor(status.userId);
        taskForm.setTaskDefKey(IacucStatus.Submit.taskDefKey());
        taskForm.setTaskName(IacucStatus.Submit.statusName());
        if (!StringUtils.isBlank(status.snapshotId)) {
            attachSnapshotToTask(IacucStatus.Submit.taskDefKey(), status, taskForm);
        }
        String taskId = processService.completeTaskByTaskForm(IacucProcessService.ProtocolProcessDefKey, taskForm);
        if (taskId == null) {
            return null;
        }
        insertToMigratorTable(taskId, status.statusId, status.statusCodeDate);
        return taskId;
    }

    public boolean hasTask(String protocolId, String taskDefKey) {
        return processService.hasTaskByTaskDefKey(protocolId, taskDefKey);
    }

    public boolean hasTaskForReviewer(String protocolId) {
        return processService.hasReviewerTask(protocolId);
    }


    public void abortProcess(String protocolId, String deletedReason) {
        log.info("process abort protocolId=" + protocolId);
        processService.deleteProcess(IacucProcessService.ProtocolProcessDefKey, protocolId, deletedReason);
    }


    private boolean attachSnapshotToTask(final String taskDefKey,
                                         final OldStatus status,
                                         final IacucTaskForm taskForm) {

        if (StringUtils.isBlank(status.snapshotId)) return false;

        try {
            this.jdbcTemplate.query(SQL_SNAPSHOT, new AbstractLobStreamingResultSetExtractor<Object>() {
                @Override
                protected void streamData(ResultSet resultSet) throws SQLException, IOException, DataAccessException {
                    Date date = resultSet.getTimestamp("CREATIONDATE");
                    InputStream is = resultSet.getBinaryStream("FILECONTEXT");
                    if (is != null) {
                        String attachmentId = processService.attachSnapshotToTask(status.protocolId, taskDefKey, is, date);
                        taskForm.setSnapshotId(attachmentId);
                    }
                }
            }, status.snapshotId);
            return true;
        } catch (Exception e) {
            log.error("attach snapshot to task: status={}", status, e);
            return false;
        }
    }

    private boolean attachAdverseSnapshotToTask(final String taskDefKey,
                                         final OldAdverseStatus status,
                                         final IacucTaskForm taskForm) {

        if (StringUtils.isBlank(status.snapshotId)) return false;

        try {
            this.jdbcTemplate.query(SQL_ADVERSE_SNAPSHOT, new AbstractLobStreamingResultSetExtractor<Object>() {
                @Override
                protected void streamData(ResultSet resultSet) throws SQLException, IOException, DataAccessException {
                    Date date = resultSet.getTimestamp("CREATIONDATE");
                    InputStream is = resultSet.getBinaryStream("FILECONTEXT");
                    if (is != null) {
                        String attachmentId = processService.attachSnapshotToAdverseEventTask(status.adverseId, taskDefKey, is, date);
                        taskForm.setSnapshotId(attachmentId);
                    }
                }
            }, status.snapshotId);
            return true;
        } catch (Exception e) {
            log.error("attach snapshot to task: status={}", status, e);
            return false;
        }
    }

    public boolean importKaputStatus(OldStatus status) {
        String processId = processService.importKaputStatus(status.protocolId, status.userId);
        if (processId != null) {
            insertToMigratorTable(processId, status.statusId, status.statusCodeDate);
            return completeKaputTask(status);
        } else {
            log.error("failed to import kaput: {}", status);
            return false;
        }
    }

    boolean completeKaputTask(OldStatus status) {

        IacucTaskForm form = new IacucTaskForm();
        form.setBizKey(status.protocolId);
        form.setAuthor(status.userId);
        form.setComment(status.statusNote);
        form.setTaskDefKey(IacucStatus.Kaput.taskDefKey());
        // name using the original status code
        String taskName = CodeToName.get(status.statusCode);
        if (taskName != null) {
            form.setTaskName(taskName);
        } else {
            form.setTaskName(status.statusCode);
        }
        //
        attachSnapshotToTask(IacucStatus.Kaput.taskDefKey(), status, form);

        // also check if it has correspondence !!!
        CorrRcd corrRcd = getCorrByCorrId(status.notificationId);
        if (corrRcd != null) {
            IacucCorrespondence corr = new IacucCorrespondence();
            corr.setCreationDate(corrRcd.creationDate);
            corr.setFrom(corrRcd.fromUserId);
            corr.setRecipient(corrRcd.to);
            corr.setSubject(corrRcd.subject);
            corr.setText(corrRcd.body);
            form.setCorrespondence(corr);
        }

        String taskId = processService.completeTaskByTaskForm(IacucProcessService.ProtocolProcessDefKey, form);
        if (taskId != null) {
            insertToMigratorTable(taskId, status.statusId, status.statusCodeDate);
            // insert correspondence date
            if (corrRcd != null) insertToAttachedCorrTable(status.statusId, corrRcd.oid, corrRcd.creationDate);
            return true;
        } else {
            return false;
        }
    }

    boolean completeKaputAdverseTask(OldAdverseStatus status) {

        IacucTaskForm form = new IacucTaskForm();
        form.setBizKey(status.adverseId);
        form.setAuthor(status.userId);
        form.setComment(status.statusNote);
        form.setTaskDefKey(IacucStatus.Kaput.taskDefKey());
        // name using the original status code
        String taskName = CodeToName.get(status.statusCode);
        if (taskName != null) {
            form.setTaskName(taskName);
        } else {
            form.setTaskName(status.statusCode);
        }
        //
        attachAdverseSnapshotToTask(IacucStatus.Kaput.taskDefKey(), status, form);

        // also check if it has correspondence !!!
        AdverseCorr corrRcd = getAdverseCorrByCorrId(status.notificationId);
        if (corrRcd != null) {
            IacucCorrespondence corr = new IacucCorrespondence();
            corr.setCreationDate(corrRcd.creationDate);
            corr.setFrom(corrRcd.fromUserId);
            corr.setRecipient(corrRcd.to);
            corr.setSubject(corrRcd.subject);
            corr.setText(corrRcd.text);
            form.setCorrespondence(corr);
        }

        String taskId = processService.completeTaskByTaskForm(IacucProcessService.AdverseEventDefKey, form);
        if (taskId != null) {
            insertToAdverseMigratorTable(taskId, status.adverseStatusId, status.statusCodeDate);
            // insert correspondence date
            if (corrRcd != null) insertToAdverseAttachedCorrTable(status.adverseStatusId, corrRcd.oid, corrRcd.creationDate);
            return true;
        } else {
            return false;
        }
    }

    public void foo() {
        CustomSqlExecution<IacucMybatisMapper, List<Map<String, Object>>>
                sqlExecution =
                new AbstractCustomSqlExecution<IacucMybatisMapper,
                        List<Map<String, Object>>>(IacucMybatisMapper.class) {

                    public List<Map<String, Object>> execute(IacucMybatisMapper customMapper) {
                        return customMapper.selectTasks();
                    }
                };

        List<Map<String, Object>> results =
                managementService.executeCustomSql(sqlExecution);
        for (Map<String, Object> m : results) {
            for (String key : m.keySet()) {
                log.info("key={}", key);
            }
        }
    }


    public void foo2() {
        CustomSqlExecution<IacucMybatisMapper, List<Map<String, Object>>>
                sqlExecution =
                new AbstractCustomSqlExecution<IacucMybatisMapper,
                        List<Map<String, Object>>>(IacucMybatisMapper.class) {

                    public List<Map<String, Object>> execute(IacucMybatisMapper customMapper) {
                        return customMapper.selectTaskWithSpecificVariable("PROTOCOL_ID");
                    }
                };

        List<Map<String, Object>> results = managementService.executeCustomSql(sqlExecution);
        for (Map<String, Object> m : results) {
            for (String k : m.keySet()) {
                log.info("Foo key=" + k);
            }
        }
    }

    /**
     * @param taskId  final String
     * @param endTime final Date
     * @return boolean
     */
    private boolean updateEndTimeByTaskId(final String taskId, final Date endTime) {
        CustomSqlExecution<IacucMybatisMapper, Boolean>
                sqlExecution =
                new AbstractCustomSqlExecution<IacucMybatisMapper,
                        Boolean>(IacucMybatisMapper.class) {
                    @Override
                    public Boolean execute(IacucMybatisMapper cmapper) {
                        cmapper.updateEndTime(taskId, endTime);
                        return true;
                    }
                };
        return managementService.executeCustomSql(sqlExecution);
    }

    public List<AttachedAppendix> getAttachedAppendix(final String protocolId) {
        List<AttachedAppendix> list = jdbcTemplate.query(SQL_APPENDIX, new RowMapper<AttachedAppendix>() {
            @Override
            public AttachedAppendix mapRow(ResultSet rs, int rowNum) throws SQLException {
                AttachedAppendix rcd = new AttachedAppendix(
                        protocolId,
                        rs.getString("APPENDIXTYPE"),
                        rs.getString("APPROVALTYPE"),
                        rs.getDate("APPROVALDATE")
                );
                return rcd;
            }
        }, protocolId);
        return list;
    }


    public ReviewRcd getReviewRcdByStatusId(String statusId) {
        List<ReviewRcd> list = jdbcTemplate.query(SQL_RV, new RowMapper<ReviewRcd>() {
            @Override
            public ReviewRcd mapRow(ResultSet rs, int rowNum) throws SQLException {
                ReviewRcd rcd = new ReviewRcd(
                        rs.getString("OID"),
                        rs.getString("STATUSID"),
                        rs.getString("REVIEWTYPE"),
                        rs.getString("A"),
                        rs.getString("B"),
                        rs.getString("C"),
                        rs.getDate("MEETINGDATE")
                );
                return rcd;
            }
        }, statusId);
        return (list == null || list.isEmpty()) ? null : list.get(0);
    }

    public CorrRcd getCorrRcdByNotificationId(String notificationId) {
        List<CorrRcd> list = jdbcTemplate.query(SQL_CORR, new RowMapper<CorrRcd>() {
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
        }, notificationId);
        return (list == null || list.isEmpty()) ? null : list.get(0);
    }

    public AdverseCorr getAdverseCorrRcdByNotificationId(String notificationId) {
        List<AdverseCorr> list = jdbcTemplate.query(SQL_ADVERSE_CORR, new RowMapper<AdverseCorr>() {
            @Override
            public AdverseCorr mapRow(ResultSet rs, int rowNum) throws SQLException {
                AdverseCorr rcd = new AdverseCorr(
                        rs.getString("OID"),
                        rs.getString("IACUCADVERSEEVENT_OID"),
                        rs.getString("USER_ID"),
                        rs.getTimestamp("CREATIONDATE"),
                        rs.getString("RECIPIENTS"),
                        rs.getString("CARBONCOPIES"),
                        rs.getString("SUBJECT"),
                        rs.getString("CORRESPONDENCETEXT")
                );
                return rcd;
            }
        }, notificationId);
        return (list == null || list.isEmpty()) ? null : list.get(0);
    }

    public boolean importCorrRcd(CorrRcd rcd) {
        IacucCorrespondence corr = new IacucCorrespondence();
        corr.setFrom(rcd.fromUserId);
        corr.setRecipient(rcd.to);
        corr.setCarbonCopy(rcd.cc);
        corr.setSubject(rcd.subject);
        corr.setText(rcd.body);
        corr.setCreationDate(rcd.creationDate);
        //
        IacucTaskForm taskForm = new IacucTaskForm();
        taskForm.setCorrespondence(corr);
        taskForm.setBizKey(rcd.protocolId);
        taskForm.setTaskDefKey(IacucStatus.AddCorrespondence.taskDefKey());
        taskForm.setTaskName(IacucStatus.AddCorrespondence.statusName());
        taskForm.setAuthor(rcd.fromUserId);
        //
        String taskId = processService.addCorrespondence(taskForm);
        if (taskId != null) {
            insertToCorrTable(taskId, rcd.oid, rcd.creationDate);
            return true;
        }
        return false;
    }

    public boolean importOldNote(OldNote oldNote) {
        IacucTaskForm taskForm = new IacucTaskForm();
        taskForm.setBizKey(oldNote.protocolId);
        taskForm.setAuthor(oldNote.author);
        taskForm.setComment(oldNote.note);
        taskForm.setTaskDefKey(IacucStatus.AddNote.taskDefKey());
        taskForm.setTaskName(IacucStatus.AddNote.statusName());
        //
        String taskId = processService.addNote(taskForm);
        if (taskId != null) {
            insertToNoteTable(taskId, oldNote.oid, oldNote.date);
            return true;
        }
        return false;
    }

    public List<IacucTaskForm> getIacucProtocolHistory(String protocolId) {
        List<IacucTaskForm> list = new ArrayList<IacucTaskForm>();
        try {
            list = processService.getIacucProtocolHistory(protocolId);
        } catch (Exception e) {
            log.error("caught exception:", e);
        }
        return list;
    }

    public List<IacucTaskForm> getIacucAdverseHistory(String adverseId) {
        List<IacucTaskForm> list = new ArrayList<IacucTaskForm>();
        try {
            list = processService.getIacucAdverseHistory(adverseId);
        } catch (Exception e) {
            log.error("caught exception:", e);
        }
        return list;
    }


    public void migrateAdverseInProgress(Deque<OldAdverseStatus> list) {
        for (OldAdverseStatus status : list) {
            migrateAdverse(status);
        }
    }

    private void migrateAdverse(OldAdverseStatus status) {

    }

}
