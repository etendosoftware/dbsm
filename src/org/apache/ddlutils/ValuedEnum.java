package org.apache.ddlutils;

import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang3.ClassUtils;

public abstract class ValuedEnum extends Enum {
   private static final long serialVersionUID = -7129650521543789085L;
   private final int iValue;

   protected ValuedEnum(String name, int value) {
      super(name);
      this.iValue = value;
   }

   protected static Enum getEnum(Class enumClass, int value) {
      if (enumClass == null) {
         throw new IllegalArgumentException("The Enum Class must not be null");
      } else {
         List list = Enum.getEnumList(enumClass);
         Iterator it = list.iterator();

         ValuedEnum enumeration;
         do {
            if (!it.hasNext()) {
               return null;
            }

            enumeration = (ValuedEnum)it.next();
         } while(enumeration.getValue() != value);

         return enumeration;
      }
   }

   public final int getValue() {
      return this.iValue;
   }

   public int compareTo(Object other) {
      return this.iValue - ((ValuedEnum)other).iValue;
   }

   public String toString() {
      if (this.iToString == null) {
         String shortName = ClassUtils.getShortClassName(this.getEnumClass());
         this.iToString = shortName + "[" + this.getName() + "=" + this.getValue() + "]";
      }

      return this.iToString;
   }
}
