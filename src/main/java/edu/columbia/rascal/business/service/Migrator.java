package edu.columbia.rascal.business.service;

import edu.columbia.rascal.batch.iacuc.AttachedAppendix;
import edu.columbia.rascal.batch.iacuc.CorrRcd;
import edu.columbia.rascal.batch.iacuc.OldStatus;
import edu.columbia.rascal.batch.iacuc.ReviewRcd;

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


/**
 * kaput the status should cover ALL names in the view history page
 */
@Service
public class Migrator {

    private static final Logger log = LoggerFactory.getLogger(Migrator.class);

    private static final String SQL_INSERT_MIGRATOR = "insert into IACUC_MIGRATOR (TASKID_, STATUSID_, DATE_) VALUES (?, ?, ?)";
    private static final String SQL_INSERT_CORR = "insert into IACUC_CORR (TASKID_, STATUSID_, DATE_) VALUES (?, ?, ?)";
    private static final String SQL_INSERT_IMI = "insert into IACUC_IMI (POID_, STATUSID_) VALUES (?, ?)";

    private static final String SQL_SNAPSHOT = "select CREATIONDATE, FILECONTEXT from IACUCPROTOCOLSNAPSHOT where ID=?";
    //
    private static final String SQL_CORR = "select OID, IACUCPROTOCOLHEADERPER_OID protocolId,  USER_ID, CREATIONDATE, RECIPIENTS, CARBONCOPIES, SUBJECT, CORRESPONDENCETEXT" +
            " from IacucCorrespondence c, RASCAL_USER u where c.AUTHORRID=u.RID and c.OID=?";
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


    private final JdbcTemplate jdbcTemplate;
    private static final Set<String> VerifySet = new HashSet<String>();

    static {
        VerifySet.add("Submit");
        VerifySet.add("Distribute");
        VerifySet.add("ReturnToPI");
        VerifySet.add("Approve");
        VerifySet.add("Done");
        VerifySet.add("Suspend");
        VerifySet.add("Terminate");
        VerifySet.add("Withdraw");
        VerifySet.add("Reinstate");
    }

    private static final Set<String> PassSet = new HashSet<String>();

    static {
        PassSet.add("ACCMemberHold");
        PassSet.add("ACCMemberApprov");
    }

    @Resource
    private ManagementService managementService;
    @Resource
    private IacucProcessService processService;

    private Date SubmitDate_;

    @Autowired
    public Migrator(JdbcTemplate jt) {
        this.jdbcTemplate = jt;
    }

    public void startup() {
        CorrRcd corr = getCorrRcdByNotificationId("5");
        log.info("corr: subject={}", corr.subject);
        ReviewRcd reviewRcd = getReviewRcdByStatusId("237");
        log.info("reviewRcd: reviewType={}", reviewRcd.reviewType);
        List<IacucTaskForm> list = getIacucProtocolHistory("166");
        for (IacucTaskForm hs : list) {
            log.info(hs.getTaskName() + ", " + hs.getEndTimeString());
        }
    }

    public void importKaput(List<OldStatus> kaputList) {
        for (OldStatus status : kaputList) {
            if (!importKaputStatus(status)) {
                log.error("err in importKaput: " + status);
            }
        }
    }

    private void insertToMigratorTable(String taskId, String statusId, Date date) {
        this.jdbcTemplate.update(SQL_INSERT_MIGRATOR, taskId, statusId, date);
    }

    private void insertToCorrTable(String taskId, String statusId, Date date) {
        this.jdbcTemplate.update(SQL_INSERT_CORR, taskId, statusId, date);
    }

    public void insertToImiTable(String protocolId, String statusId) {
        this.jdbcTemplate.update(SQL_INSERT_IMI, protocolId, statusId);
    }

    public void migration(List<OldStatus> list) {
        for (OldStatus status : list) {
            migration(status);
        }
    }

    private void migration(OldStatus status) {

        if (IacucStatus.Submit.isStatus(status.statusCode)) {
            submitProtocol(status);
        } else if ("Distribute".equals(status.statusCode)) {
            // ugly case distribution for approval
            distributeProtocol(status);
        } else if (IacucStatus.ReturnToPI.isStatus(status.statusCode)) {
            log.error("err in doReturnToPi: " + status);
        } else if ("ACCMemberHold".equals(status.statusCode)) {
            if (!completeAssigneeHoldReview(status)) {
                log.error("err in completeAssigneeHoldReview: " + status);
            }
        } else if ("ACCMemberApprov".equals(status.statusCode)) {
            if (!completeAssigneeApprovalReview(status)) {
                log.error("err in completeAssigneeApprovalReview: " + status);
            }
        } else if (IacucStatus.Suspend.isStatus(status.statusCode)) {
            log.error("err in suspendProtocol: " + status);
        } else if (IacucStatus.Terminate.isStatus(status.statusCode)) {
            log.error("err in terminateProtocol: " + status);
        } else if (IacucStatus.Withdraw.isStatus(status.statusCode)) {
            log.error("err in withdrawProtocol: " + status);
        } else if (IacucStatus.Reinstate.isStatus(status.statusCode)) {
            log.error("err in reinstateProtocol: " + status);
        } else if (IacucStatus.FinalApproval.isStatus(status.statusCode)) {
            log.error("err in doApprovalWithoutDone: " + status);
        }

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
        } else if (IacucStatus.ReturnToPI.isStatus(status.statusCode)) {
            log.error("err in doReturnToPi: " + status);
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
        } else if (IacucStatus.Suspend.isStatus(status.statusCode)) {
            log.error("err in suspendProtocol: " + status);
        } else if (IacucStatus.Terminate.isStatus(status.statusCode)) {
            log.error("err in terminateProtocol: " + status);
        } else if (IacucStatus.Withdraw.isStatus(status.statusCode)) {
            log.error("err in withdrawProtocol: " + status);
        } else if (IacucStatus.Reinstate.isStatus(status.statusCode)) {
            log.error("err in reinstateProtocol: " + status);
        } else if (IacucStatus.FinalApproval.isStatus(status.statusCode)) {
            log.error("err in doApprovalWithoutDone: " + status);
        } else {
            log.error("unhandled status: " + status);
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

        if (reviewRcd.meetingDate != null) {
            if (!hasTask(status.protocolId, IacucStatus.DistributeSubcommittee.taskDefKey())) {
                log.error("no subcommittee for protocolId={} ",
                        status.protocolId);
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
            CorrRcd corrRcd = null;
            if (!StringUtils.isBlank(status.notificationId)) {
                corrRcd = getCorrRcdByNotificationId(status.notificationId);
                if (corrRcd != null) {
                    IacucCorrespondence corr = new IacucCorrespondence();
                    corr.setCreationDate(corrRcd.creationDate);
                    corr.setFrom(corrRcd.fromUserId);
                    corr.setRecipient(corrRcd.to);
                    corr.setSubject(corrRcd.subject);
                    corr.setText(corrRcd.body);
                    form.setCorrespondence(corr);
                }
            }

            String taskId = processService.completeTaskByTaskForm(IacucProcessService.ProtocolProcessDefKey, form);
            if (taskId != null) {
                insertToMigratorTable(taskId, status.statusId, status.statusCodeDate);
                if (corrRcd != null) insertToCorrTable(taskId, corrRcd.oid, corrRcd.creationDate);
                return true;
            } else {
                return false;
            }

        }
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
            return startProcess(status, processInputMap);
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

    private boolean startProcess(OldStatus status, Map<String, Object> processInputMap) {
        String processId = processService.startProtocolProcess(status.protocolId, status.userId, processInputMap);
        insertToMigratorTable(processId, status.statusId, status.statusCodeDate);
        this.SubmitDate_ = status.statusCodeDate;
        IacucTaskForm taskForm = new IacucTaskForm();
        taskForm.setBizKey(status.protocolId);
        taskForm.setAuthor(status.userId);
        taskForm.setTaskDefKey(IacucStatus.Submit.taskDefKey());
        taskForm.setTaskName(IacucStatus.Submit.statusName());
        if (!StringUtils.isBlank(status.snapshotId)) {
            attachSnapshotToTask(IacucStatus.Submit.taskDefKey(), status, taskForm);
        }
        processService.completeTaskByTaskForm(IacucProcessService.ProtocolProcessDefKey, taskForm);
        return true;
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


    public boolean terminateProtocol(OldStatus status) {
        if (!processService.terminateProtocol(status.protocolId, status.userId)) {
            log.error("unable to start termination process: " + status);
            return false;
        }
        IacucTaskForm taskForm = new IacucTaskForm();
        if (!StringUtils.isBlank(status.snapshotId)) {
            attachSnapshotToTask(IacucStatus.Terminate.taskDefKey(), status, taskForm);
        }
        taskForm.setAuthor(status.userId);
        taskForm.setBizKey(status.protocolId);
        taskForm.setTaskDefKey(IacucStatus.Terminate.taskDefKey());
        taskForm.setTaskName(IacucStatus.Terminate.statusName());
        taskForm.setComment(status.statusNote);
        String taskId = processService.completeTaskByTaskForm(IacucProcessService.ProtocolProcessDefKey, taskForm);
        if (taskId != null) {
            insertToMigratorTable(taskId, status.statusId, status.statusCodeDate);
            return true;
        } else {
            log.error("failed to complete termination task: " + status);
            return false;
        }
    }

    public boolean suspendProtocol(OldStatus status) {

        if (!processService.suspendProtocol(status.protocolId, status.userId)) {
            log.error("unable to start suspension process: " + status);
            return false;
        }
        IacucTaskForm taskForm = new IacucTaskForm();
        taskForm.setAuthor(status.userId);
        taskForm.setComment(status.statusNote);
        taskForm.setBizKey(status.protocolId);
        taskForm.setTaskDefKey(IacucStatus.Suspend.taskDefKey());
        taskForm.setTaskName(IacucStatus.Suspend.statusName());
        if (!StringUtils.isBlank(status.snapshotId)) {
            attachSnapshotToTask(IacucStatus.Suspend.taskDefKey(), status, taskForm);
        }
        String taskId = processService.completeTaskByTaskForm(IacucProcessService.ProtocolProcessDefKey, taskForm);
        if (taskId != null) {
            insertToMigratorTable(taskId, status.statusId, status.statusCodeDate);
            return true;
        } else {
            log.error("failed to complete suspension task, status: " + status);
            return false;
        }
    }

    private boolean reinstateProtocol(OldStatus status) {

        if (!processService.reinstateProtocol(status.protocolId, status.userId)) {
            log.error("unable to start reinstate process: " + status);
            return false;
        }
        IacucTaskForm taskForm = new IacucTaskForm();
        taskForm.setAuthor(status.userId);
        taskForm.setBizKey(status.protocolId);
        taskForm.setTaskDefKey(IacucStatus.Reinstate.taskDefKey());
        taskForm.setTaskName(IacucStatus.Reinstate.statusName());
        taskForm.setComment(status.statusNote);
        if (!StringUtils.isBlank(status.snapshotId)) {
            attachSnapshotToTask(IacucStatus.Reinstate.taskDefKey(), status, taskForm);
        }
        String taskId = processService.completeTaskByTaskForm(IacucProcessService.ProtocolProcessDefKey, taskForm);
        if (taskId != null) {
            insertToMigratorTable(taskId, status.statusId, status.statusCodeDate);
            return true;
        }
        log.error("failed to complete reinstate task: " + status);
        return false;
    }

    public boolean withdrawProtocol(OldStatus status) {

        if (!processService.withdrawProtocol(status.protocolId, status.userId)) {
            log.error("unable to start withdraw process: " + status);
            return false;
        }
        IacucTaskForm taskForm = new IacucTaskForm();
        taskForm.setAuthor(status.userId);
        taskForm.setBizKey(status.protocolId);
        taskForm.setTaskDefKey(IacucStatus.Withdraw.taskDefKey());
        taskForm.setTaskName(IacucStatus.Withdraw.statusName());
        taskForm.setComment(status.statusNote);

        if (!StringUtils.isBlank(status.snapshotId)) {
            attachSnapshotToTask(IacucStatus.Withdraw.taskDefKey(), status, taskForm);
        }
        String taskId = processService.completeTaskByTaskForm(IacucProcessService.ProtocolProcessDefKey, taskForm);
        if (taskId != null) {
            insertToMigratorTable(taskId, status.statusId, status.statusCodeDate);
            return true;
        } else {
            log.error("failed to complete withdrawal task: " + status);
            return false;
        }
    }


    public boolean importKaputStatus(OldStatus status) {
        IacucTaskForm form = new IacucTaskForm();
        form.setBizKey(status.protocolId);
        form.setAuthor(status.userId);
        form.setComment(status.statusNote);
        form.setTaskDefKey(IacucStatus.Kaput.taskDefKey());
        // name using the original status code
        form.setTaskName(status.statusCode);

        String processId = processService.importKaputStatus(status.protocolId, status.userId);
        if (processId != null) {
            insertToMigratorTable(processId, status.statusId, status.statusCodeDate);
        } else {
            return false;
        }
        //
        attachSnapshotToTask(IacucStatus.Kaput.taskDefKey(), status, form);

        //
        CorrRcd corrRcd = null;
        if (!StringUtils.isBlank(status.notificationId)) {
            corrRcd = getCorrRcdByNotificationId(status.notificationId);
            if (corrRcd != null) {
                IacucCorrespondence corr = new IacucCorrespondence();
                corr.setCreationDate(corrRcd.creationDate);
                corr.setFrom(corrRcd.fromUserId);
                corr.setRecipient(corrRcd.to);
                corr.setSubject(corrRcd.subject);
                corr.setText(corrRcd.body);
                form.setCorrespondence(corr);
            }
        }

        String taskId = processService.completeTaskByTaskForm(IacucProcessService.ProtocolProcessDefKey, form);
        if (taskId != null) {
            insertToMigratorTable(taskId, status.statusId, status.statusCodeDate);
            if (corrRcd != null) insertToCorrTable(taskId, corrRcd.oid, corrRcd.creationDate);
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
            for (String k : m.keySet()) {
                log.info("key==================" + k);
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
        return list.get(0);
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
        return list.get(0);
    }

    private boolean saveCorrRcd(CorrRcd rcd) {
        return importCorrRcd(rcd);
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
        String taskId = processService.completeTaskByTaskForm(IacucProcessService.ProtocolProcessDefKey, taskForm);
        if (taskId != null) {
            insertToCorrTable(taskId, rcd.oid, rcd.creationDate);
            return true;
        }
        return false;
    }

    public List<IacucTaskForm> getIacucProtocolHistory(String protocolId) {
        List<IacucTaskForm> list = new ArrayList<IacucTaskForm>();
        try {
            log.info("startTime=" + new Date());
            list = processService.getIacucProtocolHistory(protocolId);
            log.info("endTime=" + new Date());
        } catch (Exception e) {
            log.error("caught exception:", e);
        }
        return list;
    }

}
