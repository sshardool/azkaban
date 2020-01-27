package cloudflow.servlets;

import azkaban.server.HttpRequestUtils;
import azkaban.server.session.Session;
import azkaban.webapp.AzkabanWebServer;
import azkaban.webapp.servlet.LoginAbstractAzkabanServlet;
import cloudflow.services.ExecutionParameters;
import cloudflow.services.ExecutionParameters.FailureAction;
import cloudflow.services.ExecutionParameters.ConcurrentOption;
import cloudflow.services.ExecutionService;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.HashMap;

public class ExecutionServlet extends LoginAbstractAzkabanServlet {

    public static final String FLOW_ID_PARAM = "flow_id";
    public static final String FLOW_VERSION_PARAM = "flow_version";
    public static final String DESCRIPTION_PARAM = "description";
    public static final String EXPERIMENT_ID_PARAM = "experiment_id";
    public static final String FAILURE_ACTION_PARAM = "failure_action";
    public static final String NOTIFY_FAILURE_IMMEDIATELY_PARAM = "notify_failure_immediately";
    public static final String NOTIFY_FAILURE_AT_END_PARAM = "notify_failure_at_end";
    public static final String CONCURRENT_OPTION_PARAM = "concurrent_option";

    private static final Logger logger = LoggerFactory.getLogger(ExecutionServlet.class);
    private ExecutionService executorService;
    private ObjectMapper objectMapper;

    @Override
    public void init(final ServletConfig config) throws ServletException {
        super.init(config);
        final AzkabanWebServer server = (AzkabanWebServer) getApplication();
        this.executorService = server.getExecutionService();
        this.objectMapper = server.objectMapper();
    }

    @Override
    protected void handleGet(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException, IOException {

    }

    @Override
    protected void handlePost(HttpServletRequest req, HttpServletResponse resp, Session session) throws ServletException, IOException {
        // check uri and forward to the right method

        final HashMap<String, Object> responseObj = new HashMap<>();
        ExecutionParameters executionParameters;
        try {
            executionParameters = setExecutionParameters(req, session);
        } catch (Exception e) {
            // TODO: Improve error handling
            responseObj.put("error", HttpServletResponse.SC_BAD_REQUEST);
            responseObj.put("message", "Bad Request.");
            sendResponse(resp, responseObj, HttpServletResponse.SC_BAD_REQUEST);
            return;
        }

        final String executionId = executorService.createExecution(executionParameters);
        // TODO: set location header
        sendResponse(resp, responseObj, HttpServletResponse.SC_CREATED);
    }

    private void sendResponse(HttpServletResponse httpServletRespObject,
        HashMap<String, Object> responseObject, int status) throws IOException {
        httpServletRespObject.setStatus(status);
        this.writeJSON(httpServletRespObject, responseObject);
    }


    private ExecutionParameters setExecutionParameters(final HttpServletRequest req,
        Session session) throws ServletException {
        ExecutionParameters executionParameters = new ExecutionParameters();

        // required
        executionParameters.setFlowId(getParam(req, FLOW_ID_PARAM));
        executionParameters.setFlowVersion(HttpRequestUtils.getIntParam(req, FLOW_VERSION_PARAM));
        executionParameters.setSubmitUser(session.getUser().getUserId());

        // optional
        executionParameters.setDescription(HttpRequestUtils.getParam(req, DESCRIPTION_PARAM, ""));
        executionParameters.setExperimentId(HttpRequestUtils.getParam(req, EXPERIMENT_ID_PARAM, ""));

        try {
            final FailureAction failureAction = FailureAction.valueFromName(
                    HttpRequestUtils.getParam(req, FAILURE_ACTION_PARAM, "finishCurrent"));
            executionParameters.setFailureAction(failureAction);
        } catch (IllegalArgumentException e) {
            // TODO: send bad request response
        }

        executionParameters.setNotifyOnFirstFailure(HttpRequestUtils.getBooleanParam(req,
                NOTIFY_FAILURE_IMMEDIATELY_PARAM, true));
        executionParameters.setNotifyFailureOnExecutionComplete(HttpRequestUtils.getBooleanParam(req,
                NOTIFY_FAILURE_AT_END_PARAM, false));

        try {
            final ConcurrentOption concurrentOption = ConcurrentOption.valueFromName(
                    HttpRequestUtils.getParam(req, CONCURRENT_OPTION_PARAM, "skip"));
            executionParameters.setConcurrentOption(concurrentOption);
        } catch (IllegalArgumentException e) {
            // TODO: send bad request response
        }

        return executionParameters;
    }
}
