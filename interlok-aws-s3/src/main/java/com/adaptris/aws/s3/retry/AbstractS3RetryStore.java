package com.adaptris.aws.s3.retry;

import com.adaptris.core.AdaptrisConnection;
import com.adaptris.core.CoreException;
import com.adaptris.core.http.jetty.retry.RetryStore;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.interlok.InterlokException;
import com.adaptris.interlok.util.Args;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

public abstract class AbstractS3RetryStore implements RetryStore {

    protected static final String PAYLOAD_FILE_NAME = "payload.blob";
    protected static final String METADATA_FILE_NAME = "metadata.properties";
    protected static final String STACKTRACE_FILENAME = "stacktrace.txt";

    /**
     * Set the connection to use to connect to S3.
     *
     */
    @Valid
    @Getter
    @Setter
    @NotNull
    @NonNull
    public AdaptrisConnection connection;
    @Getter
    @Setter
    @NotBlank
    public String bucket;
    @Getter
    @Setter
    public String prefix;

    public transient Pattern nameMapper = null;

    @Override
    public void prepare() throws CoreException {
        Args.notBlank(bucket, "bucket");
        LifecycleHelper.prepare(getConnection());
    }

    @Override
    public void init() throws CoreException {
        nameMapper = Pattern.compile(payloadBlobRegexp());
        LifecycleHelper.init(getConnection());

    }

    @Override
    public void start() throws CoreException {
        LifecycleHelper.start(getConnection());

    }

    @Override
    public void stop() {
        LifecycleHelper.stop(getConnection());

    }

    @Override
    public void close() {
        LifecycleHelper.close(getConnection());
    }

    @Override
    public String getStackTrace(String msgId) throws InterlokException {
        try {
            String stacktraceName = buildObjectName(msgId, STACKTRACE_FILENAME);
            try (InputStream in = getInputStream(stacktraceName)) {
                return IOUtils.toString(in, StandardCharsets.UTF_8);
            }
        } catch (Exception e) {
            throw ExceptionHelper.wrapInterlokException(e);
        }
    }

    public String buildObjectName(String msgId, String name) {
        if (StringUtils.isBlank(getPrefix())) {
            return String.format("%s/%s", msgId, name);
        }
        return String.format("%s/%s/%s", getPrefix(), msgId, name);
    }

    public String payloadBlobRegexp() {
        if (StringUtils.isBlank(getPrefix())) {
            return String.format("(.*)/%s", PAYLOAD_FILE_NAME);
        }
        return String.format("%s/(.*)/%s", getPrefix(), PAYLOAD_FILE_NAME);
    }

    protected abstract InputStream getInputStream(String objectName) throws Exception;
}
