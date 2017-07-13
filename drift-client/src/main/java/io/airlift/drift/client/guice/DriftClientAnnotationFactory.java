/*
 * Copyright (C) 2013 Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.airlift.drift.client.guice;

import java.lang.annotation.Annotation;
import java.util.Objects;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class DriftClientAnnotationFactory
{
    private DriftClientAnnotationFactory() {}

    public static Annotation getDriftClientAnnotation(Class<?> value, Class<? extends Annotation> qualifier)
    {
        return new DriftClientAnnotation(value, qualifier);
    }

    @SuppressWarnings("ClassExplicitlyAnnotation")
    private static final class DriftClientAnnotation
            implements ForDriftClient
    {
        private final Class<?> value;
        private final Class<? extends Annotation> qualifier;

        private DriftClientAnnotation(Class<?> value, Class<? extends Annotation> qualifier)
        {
            this.value = requireNonNull(value, "value is null");
            this.qualifier = requireNonNull(qualifier, "qualifier is null");
        }

        @Override
        public Class<?> value()
        {
            return value;
        }

        @Override
        public Class<? extends Annotation> qualifier()
        {
            return qualifier;
        }

        @Override
        public Class<? extends Annotation> annotationType()
        {
            return ForDriftClient.class;
        }

        @Override
        public boolean equals(Object o)
        {
            // allow equality with any instance of the annotation
            if (!(o instanceof ForDriftClient)) {
                return false;
            }
            ForDriftClient other = (ForDriftClient) o;
            return Objects.equals(value, other.value()) &&
                    Objects.equals(qualifier, other.qualifier());
        }

        @Override
        public int hashCode()
        {
            // this is specified in java.lang.Annotation
            return ((127 * "value".hashCode()) ^ value.hashCode()) +
                    ((127 * "qualifier".hashCode()) ^ qualifier.hashCode());
        }

        @Override
        public String toString()
        {
            return format("@%s(value=%s, qualifier=%s)", ForDriftClient.class.getName(), value, qualifier);
        }
    }
}
