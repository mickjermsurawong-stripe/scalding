package com.twitter.scalding.parquet.scrooge;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.OriginalType;

import java.util.Arrays;
import java.util.List;

public class ParquetListFormatCompatibility {

  abstract static public class Rule {
    public Type elementType(Type repeatedType) {
      if (repeatedType.isPrimitive()) {
        return repeatedType;
      } else {
        return firstField(repeatedType.asGroupType());
      }
    }

    public Boolean isElementRequired(Type repeatedType) {
      return true;
    }

    public String elementName(Type repeatedType) {
      return this.elementType(repeatedType).getName();
    }

    public OriginalType elementOriginalType(Type repeatedType) {
      return this.elementType(repeatedType).getOriginalType();
    }

    abstract Boolean check(Type type);

    abstract Type createCompliantRepeatedType(Type type, String name, Boolean isElementRequired, OriginalType originalType);

  }

  private static org.apache.parquet.schema.Type firstField(GroupType groupType) {
    return groupType.getFields().get(0);
  }

  public static class RulePrimitiveElement extends Rule {

    public String constantElementName() {
      return "element";
    }

    @Override
    public Boolean check(Type repeatedType) {
      return repeatedType.isPrimitive() && repeatedType.getName().equals(this.constantElementName());
    }

    @Override
    public Type createCompliantRepeatedType(Type type, String name, Boolean isElementRequired, OriginalType originalType) {
      if (!isElementRequired) {
        throw new IllegalArgumentException("Rule 1 can only take required element");
      }
      if (!type.isPrimitive()) {
        throw new IllegalArgumentException(
            String.format("Rule 1 cannot take primitive type, but is given %s", type));
      }
      return new PrimitiveType(
          Type.Repetition.REPEATED,
          type.asPrimitiveType().getPrimitiveTypeName(),
          this.constantElementName(),
          originalType
      );
    }

  }

  public static class RulePrimitiveArray extends RulePrimitiveElement {
    @Override
    public String constantElementName() {
      return "array";
    }
  }

  public static class RuleGroupElement extends Rule {
    // optional group my_list (LIST) {
    //   repeated group element {
    //     required binary str (UTF8);
    //     required int32 num;
    //   };
    // }

    private static String ELEMENT_NAME = "element";

    public Type elementType(Type repeatedType) {
      return repeatedType;
    }

    @Override
    public String elementName(Type repeatedType) {
      return ELEMENT_NAME;
    }

    @Override
    public Boolean check(Type repeatedType) {
      if (repeatedType.isPrimitive()) {
        return false;
      } else {
        GroupType repeatedGroup = repeatedType.asGroupType();
        return repeatedGroup.getFields().size() > 1 && repeatedGroup.getName().equals(ELEMENT_NAME);
      }
    }

    @Override
    public Type createCompliantRepeatedType(Type type, String name, Boolean isElementRequired, OriginalType originalType) {
      if (!type.isPrimitive()) {
        throw new IllegalArgumentException("Rule 2 can only take group type");
      }
      return new GroupType(
          Type.Repetition.REPEATED,
          ELEMENT_NAME,
          type.asGroupType().getFields()
      );
    }
  }

  public static class RulePrimitiveTuple extends Rule {

    @Override
    public Boolean check(Type type) {
      Type elementType = this.elementType(type);
      if (elementType.isPrimitive()) {
        return false;
      } else {
        GroupType repeatedGroup = elementType.asGroupType();
        return repeatedGroup.getFields().size() == 1 && repeatedGroup.getName().endsWith("_tuple");
      }
    }

    @Override
    public Type createCompliantRepeatedType(Type type, String name, Boolean isElementRequired, OriginalType originalType) {
      if (type.isPrimitive()) {
        throw new IllegalArgumentException("Rule 3 can only take group type");
      }
      if (!name.endsWith("_tuple")) {
        name = name + "_tuple";
      }
      return new GroupType(
          Type.Repetition.REPEATED,
          name,
          type.asGroupType().getFields()
      );
    }
  }

  public static class RuleGroupTuple extends Rule {

    @Override
    public Boolean check(Type type) {
      Type repeatedField = this.elementType(type);
      return repeatedField.isPrimitive() && repeatedField.getName().endsWith("_tuple");
    }

    @Override
    public Type createCompliantRepeatedType(Type type, String name, Boolean isElementRequired, OriginalType originalType) {
      if (!type.isPrimitive()) {
        throw new IllegalArgumentException("Rule 3 can only take group type");
      }
      if (!name.endsWith("_tuple")) {
        name = name + "_tuple";
      }
      return new PrimitiveType(
          Type.Repetition.REPEATED,
          type.asPrimitiveType().getPrimitiveTypeName(),
          name,
          originalType
      );
    }
  }

  public static class RuleStandardThreeLevel extends Rule {
    // repeated group list {
    //   optional binary element (UTF8);
    // }
    //
    // repeated group list {
    //   required binary element (UTF8);
    // }
    @Override
    public Boolean check(Type repeatedField) {
      if (repeatedField.isPrimitive() || !repeatedField.getName().equals("list")) {
        return false;
      }
      Type elementType = firstField(repeatedField.asGroupType());
      return elementType.getName().equals("element");
    }

    @Override
    public String elementName(Type repeatedType) {
      return "element";
    }

    @Override
    public Type createCompliantRepeatedType(Type type, String name, Boolean isElementRequired, OriginalType originalType) {
      Type elementType = null;
      if (type.isPrimitive()) {
        elementType = new PrimitiveType(
            isElementRequired ? Type.Repetition.REQUIRED : Type.Repetition.OPTIONAL,
            type.asPrimitiveType().getPrimitiveTypeName(),
            "element",
            originalType
        );
      } else {
        elementType = new GroupType(
            isElementRequired ? Type.Repetition.REQUIRED : Type.Repetition.OPTIONAL,
            "element",
            type.asGroupType().getFields()
        );
      }

      return new GroupType(
          Type.Repetition.REPEATED,
          "list",
          Arrays.asList(elementType)
      );
    }
  }

  public Type resolveFormat(Type repeatedFileType, Type repeatedProjectedType) {

    // find rule from `fileType` and `projectedType`
    List<Rule> rules = Arrays.asList(new RulePrimitiveElement(), new RulePrimitiveArray(), new RuleGroupElement(), new RuleGroupTuple(), new RuleStandardThreeLevel());
    Rule fileTypeRule = null;
    for (Rule rule : rules) {
      if (rule.check(repeatedFileType)) {
        fileTypeRule = rule;
        break;
      }
    }
    if (fileTypeRule == null) {
      throw new RuntimeException(String.format(
          "Unexpected file parquet list format for:\n%s", repeatedFileType));
    }

    Rule projectedTypeRule = null;
    for (Rule rule : rules) {
      if (rule.check(repeatedProjectedType)) {
        projectedTypeRule = rule;
        break;
      }
    }

    if (projectedTypeRule == null) {
      throw new RuntimeException(String.format(
          "Unexpected projected parquet list format for\n:%s", repeatedProjectedType));
    }

    if (projectedTypeRule == fileTypeRule) {
      return repeatedProjectedType;
    }

    String elementName = projectedTypeRule.elementName(repeatedProjectedType);
    Type elementType = projectedTypeRule.elementType(repeatedProjectedType);
    Boolean isElementRequired = projectedTypeRule.isElementRequired(repeatedProjectedType);
    OriginalType elementOriginalType = projectedTypeRule.elementOriginalType(repeatedProjectedType);

    return fileTypeRule.createCompliantRepeatedType(
        elementType,
        elementName,
        isElementRequired,
        elementOriginalType);

  }
}
