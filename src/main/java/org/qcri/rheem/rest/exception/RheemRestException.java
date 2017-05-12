package org.qcri.rheem.rest.exception;

/**
 * Created by jlucas on 5/9/17.
 */
public class RheemRestException extends RuntimeException {

    /**
     *
     */
    private static final long serialVersionUID = -8749840246686260688L;
    //	private String message = null;
    private String type = null;
    private String errorCode = null;

    public RheemRestException() {
        super();
    }
    public RheemRestException(String message, Throwable cause, String errorCode, String type) {
        super(message, cause);
        this.errorCode = errorCode;
        this.type = type;
    }

    public RheemRestException(String message, String errorCode, String type) {
        super(message);
        this.errorCode = errorCode;
        this.type = type;
    }

    public RheemRestException(Throwable cause) {
        super(cause);
    }

    public RheemRestException(String exception) {
        super(exception);
    }

    public RheemRestException(String exception,Throwable cause) {
        super(exception,cause);
    }

    public String getErrorCode() {
        return errorCode;
    }

    public String getType() {
        return type;
    }
}