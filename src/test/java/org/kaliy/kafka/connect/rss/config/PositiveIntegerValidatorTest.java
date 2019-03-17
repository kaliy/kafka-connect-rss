package org.kaliy.kafka.connect.rss.config;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class PositiveIntegerValidatorTest {
    private PositiveIntegerValidator validator = new PositiveIntegerValidator();

    @Test
    void throwsExceptionIfValueIsNegative() {
        assertThatExceptionOfType(ConfigException.class)
                .isThrownBy(() -> validator.ensureValid("sleep.seconds", -1))
                .withMessage("Invalid value -1 for configuration sleep.seconds: sleep interval should be 0 or higher");
    }

    @Test
    void passesValidationForZero() {
        assertThatCode(() -> validator.ensureValid("sleep.seconds", 0))
                .doesNotThrowAnyException();
    }

    @Test
    void passesValidationForPositiveValue() {
        assertThatCode(() -> validator.ensureValid("sleep.seconds", 666))
                .doesNotThrowAnyException();
    }

    @Test
    void hasToStringMethodForDocumentation() {
        assertThat(validator).hasToString("0 or higher");
    }

}
