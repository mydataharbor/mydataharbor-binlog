package mydataharbor.plugin.binlog.common.exception;

/**
 * @author xulang
 * @date 2023/1/29
 */
public class BinlogException extends RuntimeException {

    public BinlogException() {
    }

    public BinlogException(String message) {
        super(message);
    }

    public BinlogException(String message, Throwable cause) {
        super(message, cause);
    }

    public BinlogException(Throwable cause) {
        super(cause);
    }

    public BinlogException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
