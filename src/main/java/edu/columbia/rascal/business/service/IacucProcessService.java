package edu.columbia.rascal.business.service;

import java.io.InputStream;
import java.util.*;
import java.util.regex.Pattern;

import javax.annotation.Resource;

import org.activiti.engine.HistoryService;
import org.activiti.engine.IdentityService;
import org.activiti.engine.RuntimeService;
import org.activiti.engine.TaskService;
import org.activiti.engine.history.HistoricProcessInstance;
import org.activiti.engine.history.HistoricTaskInstance;
import org.activiti.engine.runtime.ProcessInstance;
import org.activiti.engine.task.Attachment;
import org.activiti.engine.task.Comment;
import org.activiti.engine.task.IdentityLink;
import org.activiti.engine.task.Task;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import edu.columbia.rascal.business.service.review.iacuc.IacucCorrespondence;
import edu.columbia.rascal.business.service.review.iacuc.IacucDistributeSubcommitteeForm;
import edu.columbia.rascal.business.service.review.iacuc.IacucStatus;
import edu.columbia.rascal.business.service.review.iacuc.IacucTaskForm;

@Service
class IacucProcessService {

    public static final String ProtocolProcessDefKey = "IacucApprovalProcess";
    public static final String AdverseEventDefKey = "IacucAdverseEvent";
    private static final String START_GATEWAY = "START_GATEWAY";
    private static final String SNAPSHOT = "snapshot";
    private static final String IACUC_COORESPONDENCE = "IacucCorrespondence";
    private static final String TASK_FORM_LOOKUP_PREFIX = "iacucTaskForm";
    private static final String TASK_COMPLETED = "completed";

    private static final Logger log = LoggerFactory.getLogger(IacucProcessService.class);

    private static final Map<String,String>AppendixMap=new HashMap<String,String>();
    static {
    	AppendixMap.put("A","hasAppendixA");
    	AppendixMap.put("B","hasAppendixB");
    	AppendixMap.put("C","hasAppendixC");
    	AppendixMap.put("D","hasAppendixD");
    	AppendixMap.put("E","hasAppendixE");
    	AppendixMap.put("F","hasAppendixF");
    	AppendixMap.put("G","hasAppendixG");
    	AppendixMap.put("I","hasAppendixI");
    }
        
   	private static final String REG_PATTER="^IACUC_(\\B[A-Z]+_\\B)+[A-Z]+$";
   	private static final Pattern IacucAuthorityPattern=Pattern.compile(REG_PATTER);
   	
    @Resource
    private RuntimeService runtimeService;
    @Resource
    private TaskService taskService;
    @Resource
    private HistoryService historyService;
    @Resource
    private IdentityService identityService;

    public static String GetAppendixMapKey(String appendixType) { 	return AppendixMap.get(appendixType);  }
    public static boolean MatchIacucAuthority(String authority) {  	return IacucAuthorityPattern.matcher(authority).matches(); }
    
    @Transactional
    String startProtocolProcess(String protocolId, String userId, Map<String, Object> processInput) {
        Assert.notNull(processInput);
        if (isProtocolProcessStarted(protocolId)) {
            log.warn("Process was already started for protocolId=" + protocolId);
            return null;
        }
        processInput.put(START_GATEWAY, IacucStatus.Submit.gatewayValue());
        ProcessInstance instance = runtimeService.startProcessInstanceByKey(ProtocolProcessDefKey, protocolId, processInput);
        identityService.setAuthenticatedUserId(userId);
        runtimeService.setProcessInstanceName(instance.getProcessInstanceId(), IacucStatus.Submit.name());
        log.info("protocolId={}, activityId={}, processId={}", protocolId, instance.getActivityId(), instance.getId());
        return instance.getProcessInstanceId();
    }

    boolean isProtocolProcessStarted(String bizKey) {
        return getProtocolProcessInstance(bizKey, IacucStatus.Submit.name()) != null;
    }

    boolean hasReviewerTask(String bizKey) {
            List<Task>list= taskService.createTaskQuery()
                    .processDefinitionKey(IacucProcessService.ProtocolProcessDefKey)
                    .processInstanceBusinessKey(bizKey)
                    .taskDefinitionKeyLike("rv%").list();
            return list != null && !list.isEmpty();
    }

    @Transactional
    String attachSnapshotToTask(String protocolId, String taskDefKey, InputStream content) {
        Task task = getTaskByBizKeyAndTaskDefKey(protocolId, taskDefKey);
        if (task == null) {
            log.error("can't find task=" + taskDefKey);
            return null;
        }
        String attachmentType = "IACUC " + taskDefKey + " " + SNAPSHOT;
        // name: taskDefKey.protocolId.yyyyMMddHHmmss.pdf
        String attachmentName = taskDefKey + "." + protocolId + "." + getCurrentDateString() + ".pdf";
        String attachmentDescription = taskDefKey + " " + SNAPSHOT;

        return attachSnapshot(attachmentType,
                task.getId(),
                task.getProcessInstanceId(),
                attachmentName,
                attachmentDescription,
                content);
    }
    @Transactional
    String attachSnapshotToTask(String protocolId, String taskDefKey, InputStream content, Date date) {
        Task task = getTaskByBizKeyAndTaskDefKey(protocolId, taskDefKey);
        if (task == null) {
            log.error("can't find task=" + taskDefKey);
            return null;
        }
        String attachmentType = "IACUC " + taskDefKey + " " + SNAPSHOT;
        // name: taskDefKey.protocolId.yyyyMMddHHmmss.pdf
        String attachmentName = taskDefKey + "." + protocolId + "." + getCurrentDateString(date) + ".pdf";
        String attachmentDescription = taskDefKey + " " + SNAPSHOT;

        return attachSnapshot(attachmentType,
                task.getId(),
                task.getProcessInstanceId(),
                attachmentName,
                attachmentDescription,
                content);
    }


    private String attachSnapshot(String attachmentType, String taskId, String procId,
                                  String attachmentName, String description, InputStream content) {
        Attachment attachment = taskService.createAttachment(attachmentType,
                taskId,
                procId,
                attachmentName,
                description,
                content);
        return attachment != null ? attachment.getId() : null;
    }

    private String getCurrentDateString() {
        DateTime dt = new DateTime();
        return dt.toString("yyyyMMddHHmmss");
    }
    private String getCurrentDateString(Date date) {
        DateTime dt = new DateTime(date);
        return dt.toString("yyyyMMddHHmmss");
    }

    boolean hasTaskByTaskDefKey(String protocolId, String taskDefKey) {
        return getTaskByBizKeyAndTaskDefKey(protocolId, taskDefKey) != null;
    }

    InputStream getSnapshotContent(String attachmentId) {
        Attachment attachment = taskService.getAttachment(attachmentId);
        if (attachment != null) {
            log.info(attachment.getName());
            return taskService.getAttachmentContent(attachmentId);
        }
        return null;
    }

    IacucTaskForm getPreviousApprovedData(String protocolId) {

    	List<HistoricTaskInstance>list = historyService
                        .createHistoricTaskInstanceQuery()
                        .processDefinitionKey(IacucProcessService.ProtocolProcessDefKey)
                        .processInstanceBusinessKey(protocolId)
                        .taskDefinitionKey(IacucStatus.FinalApproval.taskDefKey())
                        .finished()
                        .includeTaskLocalVariables()
                        .orderByHistoricTaskInstanceEndTime()
                        .desc().list();
    	if( list==null || list.isEmpty() ) {
    		log.warn("couldn't get hs for protocolId={}", protocolId);
            return null;
        }
    	HistoricTaskInstance hs = list.get(0);
        IacucTaskForm form = new IacucTaskForm();
        form.setTaskDefKey(IacucStatus.FinalApproval.taskDefKey());
        form.setTaskName(hs.getName());
        form.setEndTime(hs.getEndTime());
        form.setAuthor(hs.getAssignee());
        form.setTaskId(hs.getId());
        //
        Map<String, Object> localMap = hs.getTaskLocalVariables();
        @SuppressWarnings("unchecked")
        Map<String, String> taskMap = (Map<String, String>) localMap.get(TASK_FORM_LOOKUP_PREFIX + hs.getId());
        if (taskMap != null) {
            form.setProperties(taskMap);
            form.setComment(getCommentText(form.getCommentId()));
        }
        //
        @SuppressWarnings("unchecked")
        Map<String, String> corrMap = (Map<String, String>) localMap.get(IACUC_COORESPONDENCE + hs.getId());
        if (corrMap != null && !corrMap.isEmpty()) {
            IacucCorrespondence corr = new IacucCorrespondence();
            corr.setProperties(corrMap);
            form.setCorrespondence(corr);
        }
        return form;
    }


    private String getAttachmentId(String taskId) {
        List<Attachment> list = taskService.getTaskAttachments(taskId);
        if (list == null || list.isEmpty()) return null;
        Attachment attachment = list.get(0);
        return attachment.getId();
    }

    IacucTaskForm getHistoryByTaskIdForPdfComparison(String taskId) {
    	Assert.notNull(taskId, "undefined taskId");

    	HistoricTaskInstance hs = historyService
                    .createHistoricTaskInstanceQuery()
                    .processDefinitionKey(IacucProcessService.ProtocolProcessDefKey)
                    .taskId(taskId).singleResult();
        if (hs == null) {
            log.error("cannot get HistoricTaskInstance by taskId={}", taskId);
            return null;
        }
        String taskDefKey = hs.getTaskDefinitionKey();
        String procInstanceId=hs.getProcessInstanceId();
        String protocolId = getBizKeyFromHistory(procInstanceId); 
        IacucTaskForm history = new IacucTaskForm();
        history.setBizKey(protocolId);
        history.setTaskId(taskId);
        history.setTaskDefKey(taskDefKey);
        history.setSnapshotId(getAttachmentId(hs.getId()));

        return history;
    }

    String getBizKeyFromHistory(String processInstanceId){
        HistoricProcessInstance instance=historyService.createHistoricProcessInstanceQuery().processInstanceId(processInstanceId).singleResult();
        return (instance == null) ? null : instance.getBusinessKey();
    }

    String getBizKeyFromRuntime(String processInstanceId){
        ProcessInstance instance=runtimeService.createProcessInstanceQuery().processInstanceId(processInstanceId).singleResult();
        return (instance == null) ? null : instance.getBusinessKey();
    }

    @Transactional
    void deleteSnapshotById(String attachmentId) {
        taskService.deleteAttachment(attachmentId);
    }

    Set<String> getBizKeyFromOpenTasksByAssignee(String uni) {
    	Assert.notNull(uni);
        Set<String> list = new TreeSet<String>();
        List<Task> taskList = taskService.createTaskQuery()
                    .processDefinitionKey(IacucProcessService.ProtocolProcessDefKey)
                    .includeProcessVariables()
                    .taskDefinitionKeyLike("rv%")
                    .orderByTaskCreateTime()
                    .desc().list();

        if (taskList == null) return list;
        for (Task task : taskList) {
        	if( !uni.equals(getUserIdFromIdentityLink(task.getId()))) {
        		continue;
        	}
            String bizKey=getBizKeyFromRuntime(task.getProcessInstanceId());
            if (bizKey != null) {
                list.add(bizKey);
            }
        }
        return list;
    }

    Set<String> getBizKeyFromClosedTasksByAssignee(String uni) {
        Set<String> bizKeys = new TreeSet<String>();
        List<HistoricTaskInstance> fetchList = historyService
                        .createHistoricTaskInstanceQuery()
                        .processDefinitionKey(IacucProcessService.ProtocolProcessDefKey)
                        .taskDefinitionKeyLike("rv%")
                        .finished()
                        .taskAssignee(uni)
                        .orderByHistoricTaskInstanceEndTime()
                        .desc().list();
        
        if (fetchList == null) return bizKeys;
        for (HistoricTaskInstance hs : fetchList) {
        	String protocolId = getBizKeyFromHistory(hs.getProcessInstanceId());
            if (protocolId != null) {
                bizKeys.add( protocolId );
            }
        }
        return bizKeys;
    }

    @Transactional
    boolean terminateProtocol(String protocolId, String userId) {
        if (isProtocolProcessStarted(protocolId)) {
            log.error("cannot terminate this protocol because it is process, protocolId={}", protocolId);
            return false;
        }
        Map<String, Object> processInput = new HashMap<String, Object>();
        processInput.put(START_GATEWAY, IacucStatus.Terminate.gatewayValue());
        identityService.setAuthenticatedUserId(userId);
        ProcessInstance instance = runtimeService.startProcessInstanceByKey(ProtocolProcessDefKey, protocolId, processInput);
        runtimeService.setProcessInstanceName(instance.getProcessInstanceId(), IacucStatus.Terminate.name());

        log.info("protocolId={}, activityId={}, processId={}", protocolId, instance.getActivityId(), instance.getId());
        return true;
    }

    @Transactional
    boolean suspendProtocol(String protocolId, String userId) {
        if (isProtocolProcessStarted(protocolId)) {
            log.error("cannot suspend this protocol because it is process, protocolId={}", protocolId);
            return false;
        }
        Map<String, Object> processInput = new HashMap<String, Object>();
        processInput.put(START_GATEWAY, IacucStatus.Suspend.gatewayValue());
        identityService.setAuthenticatedUserId(userId);
        ProcessInstance instance = runtimeService.startProcessInstanceByKey(ProtocolProcessDefKey, protocolId, processInput);
        runtimeService.setProcessInstanceName(instance.getProcessInstanceId(), IacucStatus.Suspend.name());

        log.info("protocolId={}, activityId={}, processId={}", protocolId, instance.getActivityId(), instance.getId());
        return true;
    }

    @Transactional
    boolean reinstateProtocol(String protocolId, String userId) {
        if (isProtocolProcessStarted(protocolId)) {
            log.error("cannot reinstate this protocol because it is in process, protocolId={}", protocolId);
            return false;
        }
        Map<String, Object> processInput = new HashMap<String, Object>();
        processInput.put(START_GATEWAY, IacucStatus.Reinstate.gatewayValue());
        identityService.setAuthenticatedUserId(userId);
        ProcessInstance instance = runtimeService.startProcessInstanceByKey(ProtocolProcessDefKey, protocolId, processInput);
        runtimeService.setProcessInstanceName(instance.getProcessInstanceId(), IacucStatus.Reinstate.name());

        log.info("protocolId={}, activityId={}, processId={}", protocolId, instance.getActivityId(), instance.getId());
        return true;
    }

    @Transactional
    boolean withdrawProtocol(String protocolId, String userId) {
        if (isProtocolProcessStarted(protocolId)) {
            log.error("cannot withdraw this protocol because it is process, protocolId={}", protocolId);
            return false;
        }
        Map<String, Object> processInput = new HashMap<String, Object>();
        processInput.put(START_GATEWAY, IacucStatus.Withdraw.gatewayValue());
        identityService.setAuthenticatedUserId(userId);
        ProcessInstance instance = runtimeService.startProcessInstanceByKey(ProtocolProcessDefKey, protocolId, processInput);
        runtimeService.setProcessInstanceName(instance.getProcessInstanceId(), IacucStatus.Reinstate.name());

        log.info("protocolId={}, activityId={}, processId={}", protocolId, instance.getActivityId(), instance.getId());
        return true;
    }

    @Transactional
    String importKaputStatus(String protocolId, String userId) {
        if (isProtocolProcessStarted(protocolId)) {
            log.error("cannot import kaput this protocol because it is process, protocolId={}", protocolId);
            return null;
        }
        Map<String, Object> processInput = new HashMap<String, Object>();
        processInput.put(START_GATEWAY, IacucStatus.Kaput.gatewayValue());
        identityService.setAuthenticatedUserId(userId);
        ProcessInstance instance = runtimeService.startProcessInstanceByKey(ProtocolProcessDefKey, protocolId, processInput);
        runtimeService.setProcessInstanceName(instance.getProcessInstanceId(), IacucStatus.Kaput.name());
        log.info("protocolId={}, activityId={}, processId={}", protocolId, instance.getActivityId(), instance.getId());
        return instance.getProcessInstanceId();
    }

    Set<String> getBizKeyHasDesignatedReviewTask() {
        Set<String> set = new TreeSet<String>();
        List<Task> taskList = taskService.createTaskQuery()
                    .processDefinitionKey(IacucProcessService.ProtocolProcessDefKey)
                    .taskDefinitionKeyLike("rv%").list();
        if (taskList == null) return set;
        for (Task task : taskList) {
        	String protocolId=getBizKeyFromRuntime(task.getProcessInstanceId());
        	if (protocolId != null) {
                set.add(protocolId);
            }
        }
        log.info("BizKey has designated review task: size={}", set.size());
        return set;
    }


    Map<String, Date> getHistoricSuspendedBizKeyAndDate() {
        Map<String, Date> map = new TreeMap<String, Date>();
        List<HistoricTaskInstance> list = historyService
                    .createHistoricTaskInstanceQuery()
                    .processDefinitionKey(IacucProcessService.ProtocolProcessDefKey)
                    .taskDefinitionKey(IacucStatus.Suspend.taskDefKey())
                    .finished()
                    .orderByHistoricTaskInstanceEndTime()
                    .desc().list();
        if( list==null ) return map;
        for (HistoricTaskInstance hs : list) {
            String protocolId=getBizKeyFromHistory(hs.getProcessInstanceId());
            if (protocolId != null) {
            	map.put(protocolId, hs.getEndTime());
            }
        }
        return map;
    }


    Map<String, Date> getAdverseEventIdSubmitDate() {
        Map<String, Date> map = new TreeMap<String, Date>();
        List<ProcessInstance> instanceList = runtimeService
                .createProcessInstanceQuery()
                .processDefinitionKey(AdverseEventDefKey)
                .list();
        if (instanceList == null || instanceList.isEmpty()) return map;
        for (ProcessInstance instance : instanceList) {
            String businessKey = instance.getBusinessKey();
            HistoricTaskInstance task = historyService.createHistoricTaskInstanceQuery()
                    .processDefinitionKey(AdverseEventDefKey)
                    .processInstanceId(instance.getProcessInstanceId())
                    .taskDefinitionKey(IacucStatus.Submit.taskDefKey())
                    .singleResult();
            if (task != null) {
                map.put(businessKey, task.getCreateTime());
            }
        }
        return map;
    }

    private String getCommentText(String commentId) {
        if (commentId == null) return null;
        Comment comment = taskService.getComment(commentId);
        return comment != null ? comment.getFullMessage() : null;
    }

    // add correspondence process
    boolean addCorrespondence(IacucTaskForm taskForm) {
        if (startAddCorrespondenceProcess(taskForm.getBizKey()) != null) {
            completeTaskByTaskForm(ProtocolProcessDefKey, taskForm);
            return true;
        }
        return false;
    }

    boolean addNote(IacucTaskForm taskForm) {
        if (startAddNoteProcess(taskForm.getBizKey()) == null) {
            return false;
        }
        completeTaskByTaskForm(ProtocolProcessDefKey, taskForm);
        return true;
    }

    private ProcessInstance startAddCorrespondenceProcess(String bizKey) {
        ProcessInstance instance = getCorrProcessInstance(bizKey);
        if (instance != null) {
            log.error("add correspondence process is still running, protocolId={}", bizKey);
            return null;
        }
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(START_GATEWAY, IacucStatus.AddCorrespondence.gatewayValue());
        instance = runtimeService.startProcessInstanceByKey(ProtocolProcessDefKey, bizKey, map);
        runtimeService.setProcessInstanceName(instance.getProcessInstanceId(), IacucStatus.AddCorrespondence.name());
        return instance;
    }

    private ProcessInstance startAddNoteProcess(String bizKey) {
        ProcessInstance instance = getNoteProcessInstance(bizKey);
        if (instance != null) {
            log.error("add note process is still running, protocolId={}", bizKey);
            return null;
        }
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(START_GATEWAY, IacucStatus.AddNote.gatewayValue());
        instance = runtimeService.startProcessInstanceByKey(ProtocolProcessDefKey, bizKey, map);
        runtimeService.setProcessInstanceName(instance.getProcessInstanceId(),
                IacucStatus.AddNote.name());
        return instance;
    }


    private ProcessInstance getCorrProcessInstance(String bizKey) {
        return getProcessInstanceByName(bizKey, IacucStatus.AddCorrespondence.name());
    }

    private ProcessInstance getNoteProcessInstance(String bizKey) {
        return getProcessInstanceByName(bizKey, IacucStatus.AddNote.name());
    }

    private ProcessInstance getProcessInstanceByName(String bizKey, String instanceName) {
        return runtimeService
                .createProcessInstanceQuery()
                .processDefinitionKey(ProtocolProcessDefKey)
                .processInstanceBusinessKey(bizKey)
                .processInstanceName(instanceName)
                .singleResult();
    }


    private ProcessInstance getProtocolProcessInstance(String bizKey, String instanceName) {
        return getProcessInstanceByName(bizKey, instanceName);
    }

    Map<String, Date> getBizKeyMeetingDate(Set<String> bizKeys) {
        Assert.notNull(bizKeys);
        Map<String, Date> bizKeyMeetingDate = new HashMap<String, Date>();
        List<ProcessInstance> list = runtimeService
                .createProcessInstanceQuery()
                .processDefinitionKey(ProtocolProcessDefKey)
                .processInstanceName(IacucStatus.Submit.name())
                .includeProcessVariables().list();
        for (ProcessInstance instance : list) {
            String protocolId = instance.getBusinessKey();
            if ( hasReviewerTask(protocolId) ) {
            	// don't show these in admin-queue
            	continue;
            }
            bizKeys.add(protocolId);
            Map<String, Object> map = instance.getProcessVariables();
            if (map != null) {
                Object obj = map.get("meetingDate");
                if (obj != null) {
                    bizKeyMeetingDate.put(protocolId, (Date) obj);
                }
            }
        }
        return bizKeyMeetingDate;
    }

    Date getMeetingDateByBizKey(String bizKey) {
        List<Task> list = taskService.createTaskQuery()
                .processDefinitionKey(ProtocolProcessDefKey)
                .processInstanceBusinessKey(bizKey)
                .includeProcessVariables()
                .list();
        if (list == null || list.isEmpty())
            return null;
        Task task = list.get(0);
        Map<String, Object> map = task.getProcessVariables();
        return (map.get("meetingDate") == null) ? null : (Date) map.get("meetingDate");
    }

    void deleteProcess(String processDefKey, String bizKey, String reason) {
        ProcessInstance instance = runtimeService
                .createProcessInstanceQuery()
                .processDefinitionKey(processDefKey)
                .processInstanceBusinessKey(bizKey)
                .singleResult();
        if (instance != null)
            runtimeService.deleteProcessInstance(instance.getProcessInstanceId(), reason);
    }

    
    String getTaskAssignee(String taskDefKey, String bizKey) {
        Task task = taskService.createTaskQuery()
                .processDefinitionKey(ProtocolProcessDefKey)
                .processInstanceBusinessKey(bizKey)
                .taskDefinitionKey(taskDefKey)
                .singleResult();
        if(task==null) return null;
        return getUserIdFromIdentityLink(task.getId());
    }

    
    List<IacucTaskForm> getPreviousNote(String bizKey) {
        List<IacucTaskForm> list = new ArrayList<IacucTaskForm>();
        List<HistoricTaskInstance> hsList = historyService.createHistoricTaskInstanceQuery()
                .processDefinitionKey(ProtocolProcessDefKey)
                .processInstanceBusinessKey(bizKey)
                .taskDefinitionKey(IacucStatus.AddNote.taskDefKey())
                .finished()
                .includeTaskLocalVariables()
                .orderByHistoricTaskInstanceEndTime()
                .desc()
                .list();

        if (hsList == null) return list;

        for (HistoricTaskInstance hs : hsList) {
            Map<String, Object> localMap = hs.getTaskLocalVariables();
            if (localMap == null) continue;

            @SuppressWarnings("unchecked")
            Map<String, String> map = (Map<String, String>) localMap.get(TASK_FORM_LOOKUP_PREFIX + hs.getId());
            if (map == null) continue;
            IacucTaskForm taskForm = new IacucTaskForm();
            taskForm.setTaskId(hs.getId());
            taskForm.setEndTime(hs.getEndTime());
            taskForm.setProperties(map);
            taskForm.setComment(getCommentText(taskForm.getCommentId()));
            list.add(taskForm);
        }
        return list;
    }

    @Transactional
    boolean startAdverseEventProcess(final String adverseEventId, final String userId) {
        if (getAdverseEventProcessInstance(adverseEventId) != null) {
            log.error("Process was already started for adverseEventId={}, userId={}", adverseEventId, userId);
            return false;
        }
        ProcessInstance instance = runtimeService.startProcessInstanceByKey(AdverseEventDefKey, adverseEventId);
        log.info("adverseEventId={}, activityId={}, processId={}", adverseEventId, instance.getActivityId(), instance.getId());
        runtimeService.setProcessInstanceName(instance.getProcessInstanceId(), IacucStatus.AdverseEvent.name());
        return true;
    }

    private ProcessInstance getAdverseEventProcessInstance(String bizKey) {
        return runtimeService
                .createProcessInstanceQuery()
                .processDefinitionKey(AdverseEventDefKey)
                .processInstanceBusinessKey(bizKey)
                .singleResult();
    }


    @Transactional
    String completeTaskByTaskForm(String processDefKey, IacucTaskForm iacucTaskForm) {
        Assert.notNull(iacucTaskForm);
        String bizKey = iacucTaskForm.getBizKey();
        Assert.notNull(bizKey);
        String taskDefKey = iacucTaskForm.getTaskDefKey();
        Task task = getTask(processDefKey, bizKey, taskDefKey);
        Assert.notNull(task);
        String taskId = task.getId();
        if (task.getAssignee() == null) {
            task.setAssignee(iacucTaskForm.getAuthor());
            taskService.claim(taskId, iacucTaskForm.getAuthor());
        }

        // if you want to store comment in activity task comment, then... otherwise do nothing
        String content = iacucTaskForm.getComment();
        if (content != null) {
            Comment comment = taskService.addComment(taskId, task.getProcessInstanceId(), taskDefKey, content);
            iacucTaskForm.setCommentId(comment.getId());
        }

        // attach attribute to this task
        Map<String, String> attribute = iacucTaskForm.getProperties();
        if (attribute != null && !attribute.isEmpty())
            taskService.setVariableLocal(taskId, TASK_FORM_LOOKUP_PREFIX + taskId, attribute);

        // attach correspondence to this task
        IacucCorrespondence corr = iacucTaskForm.getCorrespondence();
        if (corr != null) {
            corr.apply();
            Map<String, String> corrProperties = corr.getProperties();
            if (!corrProperties.isEmpty()) {
                taskService.setVariableLocal(taskId, IACUC_COORESPONDENCE + taskId, corrProperties);
            }
        }

        // for show business
        if (IacucStatus.DistributeSubcommittee.isDefKey(taskDefKey)) {
            if (iacucTaskForm instanceof IacucDistributeSubcommitteeForm) {
                taskService.setVariable(taskId, "meetingDate", iacucTaskForm.getDate());
            }
        }

        // determine the direction
        Map<String, Object> map = iacucTaskForm.getTaskVariables();
        if (map != null && !map.isEmpty())
            taskService.complete(taskId, map); // go left/right/middle or go ...
        else
            taskService.complete(taskId); // go straight

        return taskId;
    }


    @Transactional
    String attachSnapshotToAdverseEventTask(final String adverseEvtId, final String taskDefKey, final InputStream content) {
        Task task = getTask(AdverseEventDefKey, adverseEvtId, taskDefKey);
        if (task == null) {
            log.error("no task taskDefKey={}, adverseEvtId={}", taskDefKey, adverseEvtId);
            return null;
        }
        String attachmentType = "IACUC_ADVERSE_EVT_" + taskDefKey + "_" + SNAPSHOT;
        // name: taskDefKey.adverseEvtid.yyyyMMddHHmmss.pdf
        String attachmentName = taskDefKey + ".adverse.evt." + adverseEvtId + "." + getCurrentDateString() + ".pdf";
        String attachmentDescription = taskDefKey + " " + SNAPSHOT;
        return attachSnapshot(attachmentType,
                task.getId(),
                task.getProcessInstanceId(),
                attachmentName,
                attachmentDescription,
                content);
    }


    private Task getTask(String processDefKey, String bizKey, String taskDefKey) {
        return taskService.createTaskQuery()
                .processDefinitionKey(processDefKey)
                .processInstanceBusinessKey(bizKey)
                .taskDefinitionKey(taskDefKey).singleResult();
    }


    List<IacucTaskForm> getIacucProtocolHistory(String protocolId) {
        return getHistory(ProtocolProcessDefKey, protocolId);
    }

    List<IacucTaskForm> getIacucAdverseHistory(String aevtId) {
        return getHistory(AdverseEventDefKey, aevtId);
    }

    private List<IacucTaskForm> getHistory(String processDefKey, String bizKey) {

        List<IacucTaskForm> listIacucTaskForm = new ArrayList<IacucTaskForm>();

        List<HistoricTaskInstance> list = getHistoriceTaskInstance(processDefKey, bizKey);

        if (list == null || list.isEmpty())
            return listIacucTaskForm;

        for (HistoricTaskInstance hs : list) {
            IacucTaskForm iacucTaskForm = new IacucTaskForm();
            iacucTaskForm.setTaskId(hs.getId());
            iacucTaskForm.setEndTime(hs.getEndTime());
            //
            Map<String, Object> localMap = hs.getTaskLocalVariables();
            @SuppressWarnings("unchecked")
            Map<String, String> taskMap = (Map<String, String>) localMap.get(TASK_FORM_LOOKUP_PREFIX + hs.getId());

            // restore the original attribute
            iacucTaskForm.setProperties(taskMap);

            // two options:
            // if comment is stored in variable, then do nothing
            // if comment is stored in task comment, then as follow
            iacucTaskForm.setComment(getCommentText(iacucTaskForm.getCommentId()));

            // two options:
            // if the snapshot id is retrieved from here, then bla bla ...
            // iacucTaskForm.setSnapshotId(snapshotId);
            // if the snapshot id is pre-stored in properties, then do nothing

            // restore the original correspondence if any
            @SuppressWarnings("unchecked")
            Map<String, String> corrMap = (Map<String, String>) localMap.get(IACUC_COORESPONDENCE + hs.getId());
            if (corrMap != null && !corrMap.isEmpty()) {
                IacucCorrespondence corr = new IacucCorrespondence();
                corr.setProperties(corrMap);
                iacucTaskForm.setCorrespondence(corr);
            }

            // for the sake of old data
            if (iacucTaskForm.getTaskDefKey() == null) {
                iacucTaskForm.setTaskDefKey(hs.getTaskDefinitionKey());
            }
            if (iacucTaskForm.getTaskName() == null) {
                iacucTaskForm.setTaskName(hs.getName());
            }
            if (iacucTaskForm.getAuthor() == null) {
                iacucTaskForm.setAuthor(hs.getAssignee());
            }

            listIacucTaskForm.add(iacucTaskForm);
        }

        return listIacucTaskForm;
    }

    private List<HistoricTaskInstance> getHistoriceTaskInstance(String processDefKey, String bizKey) {
        // if taskDeleteReason="deleted", that task was closed by activity.
        // if taskDeleteReason="completed", that task was closed by user action
        return historyService
                .createHistoricTaskInstanceQuery()
                .processDefinitionKey(processDefKey)
                .processInstanceBusinessKey(bizKey).finished()
                .taskDeleteReason(TASK_COMPLETED)
                .includeTaskLocalVariables()
                .orderByHistoricTaskInstanceEndTime().desc().list();
    }
    
    boolean isAllReviewersApproved(String bizKey){
    	ProcessInstance instance=runtimeService.createProcessInstanceQuery()
    	.processDefinitionKey(IacucProcessService.ProtocolProcessDefKey)
    	.processInstanceBusinessKey(bizKey)
    	.includeProcessVariables()
    	.singleResult();
    	if( instance==null ) {
    		log.error("no instance for bizKey={}", bizKey);
    		return false;
    	}
    	Map<String,Object>map = instance.getProcessVariables();
    	boolean allRvsApproved=false;
    	if( map.get("allRvs")!=null ) {
    		allRvsApproved=(Boolean)map.get("allRvs");
    	}
    	boolean allAppendicesApproved=false;
    	if( map.get("allAppendicesApproved")!=null ) {
    		allAppendicesApproved=(Boolean)map.get("allAppendicesApproved");
    	}
    	
    	return allRvsApproved && allAppendicesApproved;
    }

    String getCurrentProtocolProcessInstanceId(String bizKey) {
        ProcessInstance instance = getProtocolProcessInstance(bizKey, IacucStatus.Submit.name());
        return instance != null ? instance.getProcessInstanceId() : null;
    }


    List<Task> getOpenTasksByBizKeyAndCandidateGroup(String bizKey, String userId, List<String> candidateGroup) {
        Assert.notNull(bizKey, "undefined bizKey");
        List<Task> retList = new ArrayList<Task>();

        List<Task> list = taskService.createTaskQuery()
                .processDefinitionKey(IacucProcessService.ProtocolProcessDefKey)
                .processInstanceBusinessKey(bizKey)
                .taskCandidateGroupIn(candidateGroup).list();
        if (list == null) return retList;
        Task returnToPai = null;
        boolean hasUndoApproval = false;
        boolean hasRvAction = hasReviewerAction(bizKey);
        for (Task task : list) {
        	String taskDefKey=task.getTaskDefinitionKey();
            if (IacucStatus.ReturnToPI.isDefKey(taskDefKey)) {
                returnToPai = task;
                continue;
            } else if (IacucStatus.UndoApproval.isDefKey(taskDefKey)) {
                hasUndoApproval = true;
            } else if (IacucStatus.Redistribute.isDefKey(taskDefKey)) {
                if (hasRvAction) continue;
            } else if (taskDefKey.startsWith("rv") ) {
            	if( !userId.equals(getUserIdFromIdentityLink(task.getId()))) {
            		continue;
            	}
            }
            retList.add(task);
        }

        if (!hasUndoApproval && returnToPai != null) {
            retList.add(returnToPai);
        }
        return retList;
    }

    boolean hasReviewerAction(String bizKey) {
        String processInstanceId = getCurrentProtocolProcessInstanceId(bizKey);
        List<HistoricTaskInstance>list = historyService
                .createHistoricTaskInstanceQuery()
                .processInstanceBusinessKey(bizKey)
                .processInstanceId(processInstanceId)
                .taskDefinitionKeyLike("rv%")
                .finished()
                .taskDeleteReason(TASK_COMPLETED).list();
        return  list!=null && !list.isEmpty();
    }

    Set<String> getReviewerUserId(String bizKey) {
    	Set<String>reviewerUserId=new TreeSet<String>();
    	List<Task> list = taskService.createTaskQuery()
    			.processDefinitionKey(ProtocolProcessDefKey)
    			.processInstanceBusinessKey(bizKey)
    			.taskDefinitionKeyLike("rv%")
    			.list();
    	if(list == null) return reviewerUserId;
    	for(Task task: list) {
    		String userId=getUserIdFromIdentityLink(task.getId());
    		if( userId != null ) {
    			reviewerUserId.add(userId);
    		}
    	}
    	return reviewerUserId;
    }
    
    private String getUserIdFromIdentityLink(String taskId) {
    	List<IdentityLink>list = taskService.getIdentityLinksForTask(taskId);
    	if( list == null ) return null;
    	for(IdentityLink link: list) {
    		String userId=link.getUserId();
    		if(userId!=null) return userId;
    	}
    	return null;
    }
    
    Set<String> getActionedReviewerUserId(String bizKey) {
    	Set<String>rvUserId=new TreeSet<String>();
    	String processInstanceId = getCurrentProtocolProcessInstanceId(bizKey);
    	if(processInstanceId==null) return rvUserId;
        List<HistoricTaskInstance> list = historyService
                .createHistoricTaskInstanceQuery()
                .processInstanceBusinessKey(bizKey)
                .processInstanceId(processInstanceId)
                .taskDefinitionKeyLike("rv%")
                .finished()
                .taskDeleteReason(TASK_COMPLETED).list();
        if(list==null) return rvUserId;
    	for(HistoricTaskInstance task: list) {
    		String userId=task.getAssignee();
    		if( userId != null ) {
    			rvUserId.add(userId);
    		}
    	}
    	return rvUserId;
    }

    void replaceReviewer(String bizKey, String newUserId, String oldUserId) {
        List<Task>taskList = taskService.createTaskQuery()
    			.processDefinitionKey(ProtocolProcessDefKey)
    			.processInstanceBusinessKey(bizKey)
    			.taskDefinitionKeyLike("rv%")
    			.includeTaskLocalVariables()
    			.list();
    	if(taskList == null) {
    		log.error("unable to replace reviewer: bizKey={},newerUser={},oldUserId={}", bizKey,newUserId,oldUserId);
    		return;
    	}
    	for(Task task: taskList) {
    		String taskId=task.getId();
    		if( oldUserId.equals(getUserIdFromIdentityLink(taskId)) ) {
    			taskService.deleteCandidateUser(taskId, oldUserId);
    			taskService.addCandidateUser(taskId, newUserId);
    		}
    	}
    }
    
    Task getTaskByBizKeyAndTaskDefKey(String bizKey, String taskDefKey) {
    	Assert.notNull(bizKey, "undefined bizKey");
    	Assert.notNull(taskDefKey, "undefined taskDefKey");
        List<Task> list = taskService.createTaskQuery()
                .processDefinitionKey(IacucProcessService.ProtocolProcessDefKey)
                .processInstanceBusinessKey(bizKey)
                .taskDefinitionKey(taskDefKey).list();
        return (list==null || list.isEmpty()) ? null : list.get(0);
    }

}
