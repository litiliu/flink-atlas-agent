package utils;

import java.lang.reflect.Field;

public class ClassUtils {

    public static <T> T getFiledValue(Object object, String filedName, Class<T> filedClass)
        throws NoSuchFieldException, IllegalAccessException {
        Field field = object.getClass().getDeclaredField(filedName);
        boolean accessible = field.isAccessible();
        field.setAccessible(true);
        Object filedValue = field.get(object);
        field.setAccessible(accessible);
        return (T) filedValue;
    }

    public static boolean isClassInstance(Object object, String clazz) {
        try {
            Class c = Class.forName(clazz);
            if (object.getClass().equals(c)) {
                return true;
            }
        } catch (Exception e) {
            return false;
        }
        return false;
    }

    public static void main(String[] args) {

    }

}
