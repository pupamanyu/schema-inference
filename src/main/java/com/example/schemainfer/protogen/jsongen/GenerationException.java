package com.example.schemainfer.protogen.jsongen;

public class GenerationException extends RuntimeException {
    private static final long serialVersionUID = -2105441912033842653L;

    public GenerationException(String message, Throwable cause) {
        super(message, cause);
    }

    public GenerationException(String message) {
        super(message);
    }

    public GenerationException(Throwable cause) {
        super(cause);
    }

    public GenerationException(String message, ClassNotFoundException cause) {
        super(message, cause);
    }
}
