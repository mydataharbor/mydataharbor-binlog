package mydataharbor.common.binlog.exception;

/**
 * 不支持的binlog事件
 * @author xulang
 * @date 2023/1/30
 */
public class NotSupportBinlogEventException extends RuntimeException {
    public NotSupportBinlogEventException() {
    }

    public NotSupportBinlogEventException(String message) {
        super(message);
    }

    public NotSupportBinlogEventException(String message, Throwable cause) {
        super(message, cause);
    }

    public NotSupportBinlogEventException(Throwable cause) {
        super(cause);
    }

    public NotSupportBinlogEventException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
