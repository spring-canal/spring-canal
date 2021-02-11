/*
 * Copyright 2020-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.canal.annotation.listener;

import org.springframework.core.annotation.AliasFor;
import org.springframework.messaging.handler.annotation.MessageMapping;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author 橙子
 * @since 2020/11/14
 */
@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
@MessageMapping
@Documented
public @interface CanalHandler {
    @AliasFor("filter")
    String value() default "";

    /**
     * When true, designate that this is the default fallback method if the payload type
     * matches no other {@link CanalHandler} method. Only one method can be so designated.
     *
     * @return true if this is the default method.
     */
    boolean isDefault() default false;

    /**
     * Worked only if filter is empty or return TRUE.
     *
     * @return condition spEL that can this handler consume the record
     */
    String filter() default "";

    /**
     * A pseudo origin data name used in SpEL expressions within this annotation to reference
     * the current origin data on message process. This allows access to
     * properties and methods within the data.
     * Default '__record'.
     * <p>
     * Example: {@code filter = "#{__record instanceof com.alibaba.otter.canal.protocol.CanalEntry.Entry}"}.
     *
     * @return the pseudo origin data name.
     */
    String recordRef() default "__record";
}
