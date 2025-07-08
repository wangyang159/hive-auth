package com.wy.exception;

/**
 * 自定义元数据权限监控类的异常
 */
public class HiveMetaStoreException extends RuntimeException {
    public HiveMetaStoreException() {
        super();
    }

    public HiveMetaStoreException(String message) {
        super(message);
    }

    public HiveMetaStoreException(String message, Throwable cause) {
        super(message, cause);
    }

    public HiveMetaStoreException(Throwable cause) {
        super(cause);
    }

    public HiveMetaStoreException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
