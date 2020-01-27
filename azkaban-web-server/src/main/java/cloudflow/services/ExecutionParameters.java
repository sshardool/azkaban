package cloudflow.services;


public class ExecutionParameters {

    private String flowId;
    private Integer flowVersion;
    private String description;
    private String experimentId;

    private String submitUser;

    private FailureAction failureAction;
    private Boolean notifyOnFirstFailure;
    private Boolean notifyFailureOnExecutionComplete;
    private ConcurrentOption concurrentOption;

    // TODO add attribute for flow/job properties

    public ExecutionParameters () {}

    public String getFlowId() {
        return flowId;
    }

    public void setFlowId(String flowId) {
        this.flowId = flowId;
    }

    public Integer getFlowVersion() {
        return flowVersion;
    }

    public void setFlowVersion(Integer flowVersion) {
        this.flowVersion = flowVersion;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getExperimentId() {
        return experimentId;
    }

    public void setExperimentId(String experimentId) {
        this.experimentId = experimentId;
    }

    public String getSubmitUser() {
        return submitUser;
    }

    public void setSubmitUser(String submitUser) {
        this.submitUser = submitUser;
    }

    public FailureAction getFailureAction() {
        return failureAction;
    }

    public void setFailureAction(FailureAction failureAction) {
        this.failureAction = failureAction;
    }

    public Boolean getNotifyOnFirstFailure() {
        return notifyOnFirstFailure;
    }

    public void setNotifyOnFirstFailure(Boolean notifyOnFirstFailure) {
        this.notifyOnFirstFailure = notifyOnFirstFailure;
    }

    public Boolean getNotifyFailureOnExecutionComplete() {
        return notifyFailureOnExecutionComplete;
    }

    public void setNotifyFailureOnExecutionComplete(Boolean notifyFailureOnExecutionComplete) {
        this.notifyFailureOnExecutionComplete = notifyFailureOnExecutionComplete;
    }

    public ConcurrentOption getConcurrentOption() {
        return concurrentOption;
    }

    public void setConcurrentOption(ConcurrentOption concurrentOption) {
        this.concurrentOption = concurrentOption;
    }


    public enum FailureAction {
        FINISH_CURRENTLY_RUNNING("finishCurrent"),
        CANCEL_ALL("cancelImmediately"),
        FINISH_ALL_POSSIBLE("finishPossible");

        private String name;
        private FailureAction(String name) {
            this.name = name;
        }

        public String getName() {
            return this.name;
        }

        public static FailureAction valueFromName(String name) {
         for(FailureAction action: FailureAction.values()) {
             if(action.name.equals(name)) {
                 return action;
             }
         }
         throw new IllegalArgumentException("No FailureAction for name " + name);
        }
    }

    public enum ConcurrentOption {
        CONCURRENT_OPTION_SKIP("skip"),
        CONCURRENT_OPTION_PIPELINE("pipeline"),
        CONCURRENT_OPTION_IGNORE("ignore");

        private String name;
        private ConcurrentOption(String name) {
            this.name = name;
        }

        public String getName() {
            return this.name;
        }

        public static ConcurrentOption valueFromName(String name) {
            for(ConcurrentOption option: ConcurrentOption.values()) {
                if(option.name.equals(name)) {
                    return option;
                }
            }
            throw new IllegalArgumentException("No ConcurrentOption for name " + name);
        }
    }
}
