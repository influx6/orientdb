/* Generated By:JJTree: Do not edit this line. OExpansion.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.command.OCommandContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ONestedProjection extends SimpleNode {
  protected List<ONestedProjectionItem> includeItems = new ArrayList<>();
  protected List<ONestedProjectionItem> excludeItems = new ArrayList<>();
  protected ONestedProjectionItem starItem;
  protected OInteger              recursion;

  public ONestedProjection(int id) {
    super(id);
  }

  public ONestedProjection(OrientSql p, int id) {
    super(p, id);
  }

  /**
   * @param expression
   * @param input
   * @param ctx
   */
  public Object apply(OExpression expression, Object input, OCommandContext ctx) {
    throw new UnsupportedOperationException();//TODO
  }


  @Override
  public void toString(Map<Object, Object> params, StringBuilder builder) {
    builder.append(":{");
    boolean first = true;
    if (starItem != null) {
      starItem.toString(params, builder);
      first = false;
    }
    for (ONestedProjectionItem item : includeItems) {
      if (!first) {
        builder.append(", ");
      }
      item.toString(params, builder);
      first = false;
    }
    for (ONestedProjectionItem item : excludeItems) {
      if (!first) {
        builder.append(", ");
      }
      item.toString(params, builder);
      first = false;
    }

    builder.append("}");
    if (recursion != null) {
      builder.append("[");
      recursion.toString(params, builder);
      builder.append("]");
    }
  }

  public ONestedProjection copy() {
    ONestedProjection result = new ONestedProjection(-1);
    result.includeItems = includeItems.stream().map(x -> x.copy()).collect(Collectors.toList());
    result.excludeItems = excludeItems.stream().map(x -> x.copy()).collect(Collectors.toList());
    result.starItem = starItem == null ? null : starItem.copy();
    result.recursion = recursion == null ? null : recursion.copy();
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    ONestedProjection that = (ONestedProjection) o;

    if (includeItems != null ? !includeItems.equals(that.includeItems) : that.includeItems != null)
      return false;
    if (excludeItems != null ? !excludeItems.equals(that.excludeItems) : that.excludeItems != null)
      return false;
    if (starItem != null ? !starItem.equals(that.starItem) : that.starItem != null)
      return false;
    return recursion != null ? recursion.equals(that.recursion) : that.recursion == null;
  }

  @Override
  public int hashCode() {
    int result = includeItems != null ? includeItems.hashCode() : 0;
    result = 31 * result + (excludeItems != null ? excludeItems.hashCode() : 0);
    result = 31 * result + (starItem != null ? starItem.hashCode() : 0);
    result = 31 * result + (recursion != null ? recursion.hashCode() : 0);
    return result;
  }
}
/* JavaCC - OriginalChecksum=a7faf9beb3c058e28999b17cb43b26f6 (do not edit this line) */
