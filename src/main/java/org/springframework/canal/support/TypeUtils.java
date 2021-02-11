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

package org.springframework.canal.support;

import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;

/**
 * A variant of {@link org.springframework.util.TypeUtils}.
 *
 * @author 橙子
 * @since 2020/1/2
 */
public final class TypeUtils {
    private TypeUtils() {
        throw new AssertionError("No TypeUtils instances for you!");
    }

    public static Type[] getBounds(TypeVariable<?> typeVariable) {
        Type[] bounds = typeVariable.getBounds();
        if (bounds.length == 0)
            bounds = new Type[]{Object.class};
        return bounds;
    }

    /**
     * @return Both bounds, the first is upper which for {@code extends} and the second is lower which for {@code super}.
     */
    public static Type[][] getBothBounds(WildcardType wildcardType) {
        Type[] upperBounds = wildcardType.getUpperBounds();
        Type[] lowerBounds = wildcardType.getLowerBounds();
        if (upperBounds.length == 0)
            upperBounds = new Type[]{Object.class};
        if (lowerBounds.length == 0)
            lowerBounds = new Type[]{null};
        return new Type[][]{upperBounds, lowerBounds};
    }

    public static boolean isAssignableBounds(Type[] lhsBounds, Type[] rhsBounds) {
        for (Type lhsBound : lhsBounds)
            for (Type rhsBound : rhsBounds)
                if (!isAssignableBound(lhsBound, rhsBound))
                    return false;
        return true;
    }

    public static boolean isAssignable(Type lhsType, Type rhsType) {
        Assert.notNull(lhsType, "Left-hand side type must not be null");
        Assert.notNull(rhsType, "Right-hand side type must not be null");
        if (rhsType.equals(lhsType) || Object.class.equals(lhsType)
                || (lhsType instanceof Class && isAssignable((Class<?>) lhsType, rhsType))
                || (lhsType instanceof GenericArrayType && isAssignable((GenericArrayType) lhsType, rhsType))
                || (lhsType instanceof ParameterizedType && isAssignable((ParameterizedType) lhsType, rhsType)))
            return true;
        if (lhsType instanceof WildcardType)
            return isAssignable((WildcardType) lhsType, rhsType);
        if (lhsType instanceof TypeVariable)
            return isAssignable((TypeVariable<?>) lhsType, rhsType);

        if (rhsType instanceof WildcardType)
            return isWildcardTypeAssignable(lhsType, (WildcardType) rhsType);
        if (rhsType instanceof TypeVariable)
            return isTypeVariableAssignable(lhsType, (TypeVariable<?>) rhsType);
        return false;
    }

    private static boolean isAssignableBound(Type lhsBound, Type rhsBound) {
        if (rhsBound == null)
            return true;
        if (lhsBound == null)
            return false;
        return isAssignable(lhsBound, rhsBound);
    }

    private static boolean isWildcardTypeAssignable(Type lhsType, WildcardType rhsType) {
        final Type[][] rhsBothBounds = getBothBounds(rhsType);
        for (Type[] rhsBounds : rhsBothBounds)
            if (rhsBounds.length != 1 || !isAssignableBound(unwrapArrayType(lhsType), rhsBounds[0]))
                return false;
        return true;
    }

    private static boolean isTypeVariableAssignable(Type lhsType, TypeVariable<?> rhsType) {
        final Type[] rhsBounds = getBounds(rhsType);
        return rhsBounds.length == 1 && isAssignableBound(unwrapArrayType(lhsType), rhsBounds[0]);
    }

    private static Type unwrapArrayType(Type type) {
        if (type instanceof Class) {
            final Class<?> clazz = (Class<?>) type;
            if (clazz.isArray())
                return clazz.getComponentType();
        }
        if (type instanceof GenericArrayType)
            return ((GenericArrayType) type).getGenericComponentType();
        return type;
    }

    private static boolean isAssignable(Class<?> lhsType, Type rhsType) {
        // Message[] <== Message[] or Message <== Message
        if (rhsType instanceof Class)
            return ClassUtils.isAssignable(lhsType, (Class<?>) rhsType);
        // Message[] <== E[]
        if (rhsType instanceof GenericArrayType && lhsType.isArray())
            return isAssignable(lhsType.getComponentType(), ((GenericArrayType) rhsType).getGenericComponentType());
        // Message <== Message<Integer>
        if (rhsType instanceof ParameterizedType)
            return isAssignable(lhsType, ((ParameterizedType) rhsType).getRawType());
        return false;
    }

    private static boolean isAssignable(GenericArrayType lhsType, Type rhsType) {
        final Type lhsComponent = lhsType.getGenericComponentType();
        // E[] <== E[]
        if (rhsType instanceof GenericArrayType)
            return isAssignable(lhsComponent, ((GenericArrayType) rhsType).getGenericComponentType());
        // E[] <== Message[]
        if (rhsType instanceof Class && ((Class<?>) rhsType).isArray())
            return isAssignable(lhsComponent, ((Class<?>) rhsType).getComponentType());
        // E[] <x= Message<Integer>
        return false;
    }

    private static boolean isAssignable(ParameterizedType lhsType, Type rhsType) {
        // Message<Integer> <== Message<Integer>
        if (rhsType instanceof ParameterizedType) {
            final Type lhsRawType = lhsType.getRawType();
            final Type rhsRawType = ((ParameterizedType) rhsType).getRawType();
            if (!isAssignable(lhsRawType, rhsRawType))
                return false;
            final Type[] lhsTypeArguments = lhsType.getActualTypeArguments();
            final Type[] rhsTypeArguments = ((ParameterizedType) rhsType).getActualTypeArguments();
            if (lhsTypeArguments.length != rhsTypeArguments.length)
                return false;
            for (int i = 0; i < lhsTypeArguments.length; i++)
                if (!isAssignable(lhsTypeArguments[i], rhsTypeArguments[i]))
                    return false;
            return true;
        }
        // Message<Integer> <x= Message
        // Message<Integer> <x= E[]
        return false;
    }

    private static boolean isAssignable(WildcardType lhsType, Type rhsType) {
        if (rhsType instanceof WildcardType)
            return isAssignable(lhsType, (WildcardType) rhsType);
        // ? extends Message <== Message
        // ? extends Message <== T[]
        // ? extends Message <== Message<Integer>
        // ? extends Message <== T extends Message<Integer>
        for (Type[] lhsBounds : getBothBounds(lhsType))
            for (Type lhsBound : lhsBounds)
                if (!isAssignableBound(lhsBound, rhsType))
                    return false;
        return true;
    }

    private static boolean isAssignable(TypeVariable<?> lhsType, Type rhsType) {
        if (rhsType instanceof TypeVariable)
            return isAssignable(lhsType, (TypeVariable<?>) rhsType);
        // T extends Message <== Message
        // T extends Message <== T[]
        // T extends Message <== Message<Integer>
        // T extends Message <== ? extends Message<Integer>
        for (Type lhsBound : getBounds(lhsType))
            if (!isAssignableBound(lhsBound, rhsType))
                return false;
        return true;
    }


    private static boolean isAssignable(WildcardType lhsType, WildcardType rhsType) {
        // ? extends Message <== ? extends Message
        for (Type[] lhsBounds : getBothBounds(lhsType))
            for (Type[] rhsBounds : getBothBounds(rhsType))
                if (!isAssignableBounds(lhsBounds, rhsBounds))
                    return false;
        return true;
    }

    private static boolean isAssignable(TypeVariable<?> lhsType, TypeVariable<?> rhsType) {
        // T extends Message <== T extends Message
        return isAssignableBounds(getBounds(lhsType), getBounds(rhsType));
    }
}
