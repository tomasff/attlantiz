package com.tomff.attlantiz.exceptions;

import java.io.IOException;

public class InvalidKeyValueRecordException extends IOException {
    public InvalidKeyValueRecordException(String msg) {
        super(msg);
    }
}
